package tariff

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/evcc-io/evcc/api"
	"github.com/evcc-io/evcc/util"
	"github.com/evcc-io/evcc/util/request"
)

const (
	oteEndpoint  = "https://www.ote-cr.cz/services/PublicDataService"
	oteNamespace = "http://www.ote-cr.cz/schema/service/public"
)

type Ote struct {
	*request.Helper
	*embed
	log  *util.Logger
	data *util.Monitor[api.Rates]
}

var _ api.Tariff = (*Ote)(nil)

func init() {
	registry.Add("ote", NewOteFromConfig)
}

func NewOteFromConfig(other map[string]any) (api.Tariff, error) {
	var cc struct {
		embed `mapstructure:",squash"`
	}

	if err := util.DecodeOther(other, &cc); err != nil {
		return nil, err
	}
	if err := cc.init(); err != nil {
		return nil, err
	}

	log := util.NewLogger("ote")
	t := &Ote{
		Helper: request.NewHelper(log),
		embed:  &cc.embed,
		log:    log,
		data:   util.NewMonitor[api.Rates](2 * time.Hour),
	}

	return runOrError(t)
}

type otePriceItem struct {
	Date             string  `xml:"Date"`
	PeriodResolution string  `xml:"PeriodResolution"`
	PeriodIndex      int     `xml:"PeriodIndex"`
	Price            float64 `xml:"Price"`
	EmergencyState   int     `xml:"EmergencyState"`
}

type otePriceEnvelope struct {
	Body struct {
		Response struct {
			Result struct {
				Items []otePriceItem `xml:"Item"`
			} `xml:"Result"`
		} `xml:"GetDamPricePeriodEResponse"`
		Fault *struct {
			FaultString string `xml:"faultstring"`
		} `xml:"Fault"`
	} `xml:"Body"`
}

type oteIndexItem struct {
	Date    string  `xml:"Date"`
	EurRate float64 `xml:"EurRate"`
}

type oteIndexEnvelope struct {
	Body struct {
		Response struct {
			Result struct {
				Items []oteIndexItem `xml:"DamIndex"`
			} `xml:"Result"`
		} `xml:"GetDamIndexEResponse"`
		Fault *struct {
			FaultString string `xml:"faultstring"`
		} `xml:"Fault"`
	} `xml:"Body"`
}

func oteEnvelope(operation, params string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:pub="%s">
  <soapenv:Header/>
  <soapenv:Body>
    <pub:%s>%s</pub:%s>
  </soapenv:Body>
</soapenv:Envelope>`, oteNamespace, operation, params, operation)
}

func (t *Ote) call(operation, body string, dst any) error {
	req, err := http.NewRequest(http.MethodPost, oteEndpoint, bytes.NewBufferString(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/xml; charset=utf-8")
	req.Header.Set("SOAPAction", oteNamespace+"/"+operation)

	resp, err := t.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}

	if err := xml.NewDecoder(resp.Body).Decode(dst); err != nil {
		return err
	}
	return nil
}

func (t *Ote) fetch(start, end time.Time) (api.Rates, error) {
	startDate := start.Format(time.DateOnly)
	endDate := end.Format(time.DateOnly)

	priceParams := fmt.Sprintf("<pub:StartDate>%s</pub:StartDate><pub:EndDate>%s</pub:EndDate><pub:PeriodResolution>PT15M</pub:PeriodResolution>", startDate, endDate)
	var prices otePriceEnvelope
	if err := t.call("GetDamPricePeriodE", oteEnvelope("GetDamPricePeriodE", priceParams), &prices); err != nil {
		return nil, err
	}
	if prices.Body.Fault != nil {
		return nil, errors.New(prices.Body.Fault.FaultString)
	}

	indexParams := fmt.Sprintf("<pub:StartDate>%s</pub:StartDate><pub:EndDate>%s</pub:EndDate>", startDate, endDate)
	var indexes oteIndexEnvelope
	if err := t.call("GetDamIndexE", oteEnvelope("GetDamIndexE", indexParams), &indexes); err != nil {
		return nil, err
	}
	if indexes.Body.Fault != nil {
		return nil, errors.New(indexes.Body.Fault.FaultString)
	}

	ratesByDate := make(map[string]float64, len(indexes.Body.Response.Result.Items))
	for _, item := range indexes.Body.Response.Result.Items {
		ratesByDate[item.Date] = item.EurRate
	}

	loc, err := time.LoadLocation("Europe/Prague")
	if err != nil {
		return nil, err
	}

	res := make(api.Rates, 0, len(prices.Body.Response.Result.Items))
	for _, item := range prices.Body.Response.Result.Items {
		if item.PeriodResolution != "PT15M" || item.PeriodIndex < 1 || item.EmergencyState != 0 {
			continue
		}
		exchangeRate, ok := ratesByDate[item.Date]
		if !ok || exchangeRate <= 0 {
			return nil, fmt.Errorf("missing EUR/CZK rate for %s", item.Date)
		}

		day, err := time.ParseInLocation(time.DateOnly, item.Date, loc)
		if err != nil {
			return nil, err
		}
		periodStart := day.Add(time.Duration(item.PeriodIndex-1) * SlotDuration)
		periodEnd := periodStart.Add(SlotDuration)
		priceCZKPerKWh := item.Price * exchangeRate / 1000

		res = append(res, api.Rate{
			Start: periodStart.Local(),
			End:   periodEnd.Local(),
			Value: t.totalPrice(priceCZKPerKWh, periodStart),
		})
	}

	if len(res) == 0 {
		return nil, errors.New("no OTE prices returned")
	}
	return res, nil
}

func (t *Ote) run(done chan error) {
	var once sync.Once

	for tick := time.Tick(time.Hour); ; <-tick {
		var data api.Rates
		if err := backoff.Retry(func() error {
			now := time.Now()
			var err error
			data, err = t.fetch(now, now.AddDate(0, 0, 1))
			if err != nil {
				return backoffPermanentError(err)
			}
			return nil
		}, bo()); err != nil {
			if reportError(&once, done, err) {
				return
			}
			t.log.ERROR.Println(err)
			continue
		}

		mergeRates(t.data, data)
		once.Do(func() { close(done) })
	}
}

func (t *Ote) Rates() (api.Rates, error) {
	var res api.Rates
	err := t.data.GetFunc(func(val api.Rates) {
		res = slices.Clone(val)
	})
	return res, err
}

func (t *Ote) Type() api.TariffType {
	return api.TariffTypePriceForecast
}
