package main

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"time"

	"net/http/cookiejar"

	"golang.org/x/net/publicsuffix"
)

type Request struct {
	Message *Message
}

type Message struct {
	Idusers               int32     `url:"id_users" json:"id_users,omitempty"`
	Idorder               int32     `url:"id_order" json:"id_order,omitempty"`
	OrderNum              string    `url:"order_num" json:"order_num,omitempty"`
	Name                  string    `url:"name" json:"name,omitempty"`
	Hash                  string    `url:"hash" json:"hash,omitempty"`
	Operant               int32     `url:"operant" json:"operant,omitempty"`
	Surname               string    `url:"surname" json:"surname,omitempty"`
	Secondname            string    `url:"second_name" json:"second_name,omitempty"`
	Birthday              time.Time `url:"-" json:"birthday,omitempty"`
	BirthdayRaw           string    `url:"birthday"`
	Email                 string    `url:"email" json:"email,omitempty"`
	Phone                 string    `url:"phone" json:"phone,omitempty"`
	City                  string    `url:"city" json:"city,omitempty"`
	Pasport               string    `url:"pasport" json:"pasport,omitempty"`
	Pasportdate           time.Time `url:"-" json:"pasportdate,omitempty"`
	PassportDateRaw       string    `url:"pasportdate"`
	Pasportissued         string    `url:"pasportissued" json:"pasportissued,omitempty"`
	Pasportdepartmentcode string    `url:"pasportdepartmentcode" json:"pasportdepartmentcode,omitempty"`
	Propdate              time.Time `url:"prop_date" json:"prop_date,omitempty"`
	Oblsubj               string    `url:"obl_subj" json:"obl_subj,omitempty"`
	Propregion            string    `url:"prop_region" json:"prop_region,omitempty"`
	Propcitytype          string    `url:"prop_city_type" json:"prop_city_type,omitempty"`
	Propzip               string    `url:"prop_zip" json:"prop_zip,omitempty"`
	Propstreettypelong    string    `url:"prop_street_type_long" json:"prop_street_type_long,omitempty"`
	Street                string    `url:"street" json:"street,omitempty"`
	Propstreettypeshort   string    `url:"prop_street_type_short" json:"prop_street_type_short,omitempty"`
	Building              string    `url:"building" json:"building,omitempty"`
	Buildingadd           string    `url:"building_add" json:"building_add,omitempty"`
	Flatadd               string    `url:"flat_add" json:"flat_add,omitempty"`
	Homephone             string    `url:"home_phone" json:"home_phone,omitempty"`
	Cityprog              string    `url:"city_prog" json:"city_prog,omitempty"`
	Progoblsubj           string    `url:"prog_obl_subj" json:"prog_obl_subj,omitempty"`
	Progregion            string    `url:"prog_region" json:"prog_region,omitempty"`
	Progcitytype          string    `url:"prog_city_type" json:"prog_city_type,omitempty"`
	Progzip               string    `url:"prog_zip" json:"prog_zip,omitempty"`
	Progstreettypelong    string    `url:"prog_street_type_long" json:"prog_street_type_long,omitempty"`
	Streetprog            string    `url:"street_prog" json:"street_prog,omitempty"`
	Progstreettypeshort   string    `url:"prog_street_type_short" json:"prog_street_type_short,omitempty"`
	Buildingprog          string    `url:"building_prog" json:"building_prog,omitempty"`
	Buildingaddprog       string    `url:"building_add_prog" json:"building_add_prog,omitempty"`
	Flataddprog           string    `url:"flat_add_prog" json:"flat_add_prog,omitempty"`
	Progphone             string    `url:"prog_phone" json:"prog_phone,omitempty"`
	Progmetro             string    `url:"prog_metro" json:"prog_metro,omitempty"`
	Phonemesto            string    `url:"phone_mesto" json:"phone_mesto,omitempty"`
	Workemail             string    `url:"work_email" json:"work_email,omitempty"`
	Workphonebuh          string    `url:"work_phone_buh" json:"work_phone_buh,omitempty"`
	Birthplace            string    `url:"birthplace" json:"birthplace,omitempty"`
	Phoneotdelmesto       string    `url:"phone_otdel_mesto" json:"phone_otdel_mesto,omitempty"`
	Citymesto             string    `url:"city_mesto" json:"city_mesto,omitempty"`
	Mestooblsubj          string    `url:"mesto_obl_subj" json:"mesto_obl_subj,omitempty"`
	Mestoregion           string    `url:"mesto_region" json:"mesto_region,omitempty"`
	Mestocitytype         string    `url:"mesto_city_type" json:"mesto_city_type,omitempty"`
	Mestozip              string    `url:"mesto_zip" json:"mesto_zip,omitempty"`
	Mestostreettypelong   string    `url:"mesto_street_type_long" json:"mesto_street_type_long,omitempty"`
	Streetmesto           string    `url:"street_mesto" json:"street_mesto,omitempty"`
	Mestostreettypeshort  string    `url:"mesto_street_type_short" json:"mesto_street_type_short,omitempty"`
	Buildingmesto         string    `url:"building_mesto" json:"building_mesto,omitempty"`
	Buildingaddmesto      string    `url:"building_add_mesto" json:"building_add_mesto,omitempty"`
	Flataddmesto          string    `url:"flat_add_mesto" json:"flat_add_mesto,omitempty"`
	Dohod                 string    `url:"dohod" json:"dohod,omitempty"`
	Dohodofficial         string    `url:"dohod_official" json:"dohod_official,omitempty"`
	Ijdivencev            string    `url:"ijdivencev" json:"ijdivencev,omitempty"`
	Sumrequest            string    `url:"sum_request" json:"sum_request,omitempty"`
	Days                  string    `url:"days" json:"days,omitempty"`
	Persents              float32   `url:"persents" json:"persents,omitempty"`
	HasError              bool      `url:"-"`
}

type Response struct {
	IdorderRaw      interface{} `json:"id_order,omitempty"`
	OrderNumRaw     interface{} `json:"order_num,omitempty"`
	IdusersRaw      interface{} `json:"id_users,omitempty"`
	PercentRaw      interface{} `json:"percent,omitempty"`
	MaxAmountRaw    interface{} `json:"sum_max,omitempty"`
	StatusRaw       interface{} `json:"status,omitempty"`
	PayStatusRaw    interface{} `json:"pay_status,omitempty"`
	ProductNameRaw  interface{} `json:"name_product,omitempty"`
	GivenAtRaw      interface{} `json:"give_date,omitempty"`
	FineDaysRaw     interface{} `json:"fine_days,omitempty"`
	NextPayAtRaw    interface{} `json:"back_date_with_prolong,omitempty"`
	TotalDebtRaw    interface{} `json:"current_total_debt,omitempty"`
	PaidTotalRaw    interface{} `json:"all_paysum,omitempty"`
	BodyDebtRaw     interface{} `json:"main_debt,omitempty"`
	PercentsDebtRaw interface{} `json:"percents_debt,omitempty"`
	FineDebtRaw     interface{} `json:"fine_debt,omitempty"`

	Idorder      string
	OrderNum     string
	Idusers      string
	Percent      float64
	MaxAmount    float64
	Status       string
	PayStatus    string
	ProductName  string
	GivenAt      time.Time
	NextPayAt    time.Time
	FineDays     int
	TotalDebt    float64
	PaidTotal    float64
	BodyDebt     float64
	PercentsDebt float64
	FineDebt     float64

	Errors string
}

type Service struct {
	url  string
	tls  bool
	auth *BasicAuth
	hash string
}

type BasicAuth struct {
	Login    string
	Password string
}

var basicAuth *BasicAuth

func NewService(url string, tls bool, auth *BasicAuth, hash string) *Service {
	basicAuth = auth

	return &Service{
		url:  url,
		tls:  tls,
		auth: auth,
		hash: hash,
	}
}

func dialTimeout(network, addr string) (net.Conn, error) {
	return net.DialTimeout(network, addr, time.Duration(30*time.Second))
}

func redirectPolicyFunc(req *http.Request, via []*http.Request) error {
	if basicAuth != nil {
		req.SetBasicAuth(basicAuth.Login, basicAuth.Password)
	}

	return nil
}

func (service *Service) Process(request *Request) (*Response, error) {
	request.Message.Hash = service.hash

	if !request.Message.Birthday.IsZero() {
		request.Message.BirthdayRaw = request.Message.Birthday.Format("02.01.2006")
	}

	if !request.Message.Pasportdate.IsZero() {
		request.Message.PassportDateRaw = request.Message.Pasportdate.Format("02.01.2006")
	}

	data := structToMap(request.Message)
	buffer := bytes.NewBufferString(data.Encode())

	log.Printf("[%v] Raw request:\n%s", time.Now(), buffer.String())

	req, err := http.NewRequest("POST", service.url, buffer)

	if err != nil {
		return nil, err
	}

	if service.auth != nil {
		req.SetBasicAuth(service.auth.Login, service.auth.Password)
	}

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))
	req.Header.Set("User-Agent", "gowsdl/0.1")
	req.Close = true

	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: service.tls,
		},
		Dial: dialTimeout,
	}

	client := &http.Client{
		Transport:     transport,
		CheckRedirect: redirectPolicyFunc,
	}

	options := cookiejar.Options{
		PublicSuffixList: publicsuffix.List,
	}

	jar, err := cookiejar.New(&options)

	if err == nil {
		client.Jar = jar
	}

	res, err := client.Do(req)

	if err != nil {
		return nil, err
	}

	defer res.Body.Close()

	rawBody, err := ioutil.ReadAll(res.Body)

	if err != nil {
		return nil, err
	}

	if len(rawBody) == 0 {
		return nil, fmt.Errorf("[%v] (!) Empty response!", time.Now())
	}

	log.Printf("[%v] Raw response:\n%s", time.Now(), string(rawBody))

	response := &Response{}
	err = json.Unmarshal(rawBody, response)

	if err != nil {
		return nil, err
	}

	response.Idorder = interfaceToString(response.IdorderRaw)
	response.Idusers = interfaceToString(response.IdusersRaw)

	if response.Idorder == "" && response.Idusers == "" {
		response.Errors = string(rawBody)
	}

	return response, nil
}

func interfaceToStringPtr(obj interface{}) *string {
	result := interfaceToString(obj)
	return &result
}

func interfaceToIntPtr(obj interface{}) *int {
	result := interfaceToInt(obj)
	return &result
}

func interfaceToFloatPtr(obj interface{}) *float64 {
	result := interfaceToFloat(obj)
	return &result
}

func interfaceToString(obj interface{}) string {
	switch obj.(type) {
	case bool:
		return "0"
	case string:
		return obj.(string)
	case int:
		return fmt.Sprintf("%d", obj.(int))
	case int64:
		return fmt.Sprintf("%d", obj.(int64))
	}

	return ""
}

func interfaceToInt(obj interface{}) int {
	switch obj.(type) {
	case string:
		i, _ := strconv.Atoi(obj.(string))
		return i
	case int64:
		return int(obj.(int64))
	case float64:
		return int(obj.(float64))
	}

	return 0
}

func interfaceToTime(obj interface{}) time.Time {
	switch obj.(type) {
	case time.Time:
		return obj.(time.Time)
	}

	return time.Time{}
}

func interfaceToFloat(obj interface{}) float64 {
	switch obj.(type) {
	case string:
		f, _ := strconv.ParseFloat(obj.(string), 64)
		return f
	case int:
		return float64(obj.(int))
	case int64:
		return float64(obj.(int64))
	}

	return 0.0
}

func structToMap(obj interface{}) (values url.Values) {
	values = url.Values{}
	valueOf := reflect.ValueOf(obj).Elem()
	typeOf := valueOf.Type()

	for i := 0; i < valueOf.NumField(); i++ {
		var value string
		field := valueOf.Field(i)

		switch field.Interface().(type) {
		case int, int8, int16, int32, int64:
			if field.Int() != 0 {
				value = strconv.FormatInt(field.Int(), 10)
			}

		case uint, uint8, uint16, uint32, uint64:
			if field.Uint() != 0 {
				value = strconv.FormatUint(field.Uint(), 10)
			}

		case float32:
			if field.Float() != 0.0 {
				value = strconv.FormatFloat(field.Float(), 'f', -1, 32)
			}

		case float64:
			if field.Float() != 0.0 {
				value = strconv.FormatFloat(field.Float(), 'f', -1, 64)
			}

		case []byte:
			value = string(field.Bytes())

		case string:
			value = field.String()

		case time.Time:
			t := field.Interface().(time.Time)

			if !t.IsZero() {
				value = t.Format("2006-01-02 15:04:05")
			}
		}

		if value != "" {
			target := typeOf.Field(i).Tag.Get("url")

			if target != "" && target != "-" {
				values.Set(target, value)
			}
		}
	}

	return values
}
