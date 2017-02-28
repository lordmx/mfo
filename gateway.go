package main

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	sq "github.com/lann/squirrel"
)

type Order struct {
	Id             int       `db:"id"`
	OrderNum       *string   `db:"order_num"`
	Hash           string    `db:"hash"`
	UserId         *int      `db:"outer_user_id"`
	ClientId       int       `db:"client_id"`
	OrderId        *int      `db:"order_id"`
	Status         string    `db:"status"`
	PayStatus      string    `db:"pay_status"`
	Percent        *float64  `db:"percent"`
	MaxAmount      *float64  `db:"max_amount"`
	ProductName    *string   `db:"product_name"`
	GivenAt        time.Time `db:"given_at"`
	NextPayAt      time.Time `db:"next_pay_at"`
	FineDays       *int      `db:"fine_days"`
	TotalDebt      *float64  `db:"total_debt"`
	PaidTotal      *float64  `db:"paid_total"`
	BodyDebt       *float64  `db:"body_debt"`
	PercentsDebt   *float64  `db:"percents_debt"`
	FineDebt       *float64  `db:"fine_debt"`
	CreatedAt      time.Time `db:"created_at"`
	ExternalNotice *string   `db:"external_notice"`
	Data           *Message  `db:"export_data"`
}

type OrdersGateway struct {
	db *sqlx.DB
}

func NewOrdersGateway(db *sqlx.DB) *OrdersGateway {
	return &OrdersGateway{
		db: db,
	}
}

func (order *Order) Bytes() []byte {
	hash := map[string]interface{}{
		"hash":            order.Hash,
		"status":          order.Status,
		"pay_status":      order.PayStatus,
		"product_name":    ptrToString(order.ProductName),
		"external_notice": ptrToString(order.ExternalNotice),
		"order_num":       ptrToString(order.OrderNum),
		"given_at":        order.GivenAt,
		"next_pay_at":     order.NextPayAt,
		"user_id":         ptrToInt(order.UserId),
		"order_id":        ptrToInt(order.OrderId),
		"fine_days":       ptrToInt(order.FineDays),
		"percent":         ptrToFloat(order.Percent),
		"max_amount":      ptrToFloat(order.MaxAmount),
		"total_debt":      ptrToFloat(order.TotalDebt),
		"paid_total":      ptrToFloat(order.PaidTotal),
		"body_debt":       ptrToFloat(order.BodyDebt),
		"percents_debt":   ptrToFloat(order.PercentsDebt),
		"fine_debt":       ptrToFloat(order.FineDebt),
	}

	result, _ := json.Marshal(hash)

	return result
}

func (data Message) Value() (driver.Value, error) {
	return json.Marshal(data)
}

func (data *Message) Scan(src interface{}) error {
	value := reflect.ValueOf(src)

	if !value.IsValid() || value.IsNil() {
		return nil
	}

	if result, ok := src.([]byte); ok {
		err := json.Unmarshal(result, &data)

		if err != nil {
			data.HasError = true
		}

		return err
	}

	return fmt.Errorf("Could not not decode type %T -> %T", src, data)
}

func (gateway *OrdersGateway) getColumns() []string {
	return []string{
		"id",
		"hash",
		"outer_user_id",
		"client_id",
		"order_id",
		"status",
		"created_at",
		"external_notice",
		"export_data",
		"pay_status",
		"product_name",
		"given_at",
		"next_pay_at",
		"percent",
		"max_amount",
		"paid_total",
		"total_debt",
		"fine_debt",
		"body_debt",
		"percents_debt",
		"order_num",
		"fine_days",
	}
}

func (gateway *OrdersGateway) FindById(id int) *Order {
	query := QueryBuilder().
		Select().
		Columns(strings.Join(gateway.getColumns(), ", ")).
		From(`"order"`).
		Where("id = ?", id)

	return gateway.fetch(query)
}

func (gateway *OrdersGateway) FindByHash(hash string) *Order {
	query := QueryBuilder().
		Select().
		Columns(strings.Join(gateway.getColumns(), ", ")).
		From(`"order"`).
		Where("hash = ?", hash)

	return gateway.fetch(query)
}

func (gateway *OrdersGateway) FindUnprocessed(limit, offset int) []*Order {
	orders := []*Order{}
	result := []*Order{}
	statuses := []string{"created", "pending"}

	query := QueryBuilder().
		Select().
		Columns(strings.Join(gateway.getColumns(), ", ")).
		From(`"order"`).
		Where(map[string]interface{}{"status": statuses}).
		Limit(uint64(limit)).
		Offset(uint64(offset)).
		OrderBy("id ASC")

	sql, params, _ := query.ToSql()
	err := gateway.db.Select(&orders, sql, params...)

	if err != nil {
		log.Printf("[%v] (!) Orders gateway (OrdersGateway::FindUnprocessed): %s", time.Now(), err.Error())
		return nil
	}

	for _, order := range orders {
		if !order.Data.HasError {
			result = append(result, order)
		} else {
			order.Status = StatusError

			sql := fmt.Sprintf(
				`UPDATE "order" SET status = '%s' WHERE id = %d`,
				order.Status,
				order.Id,
			)

			_, err := gateway.db.Exec(sql)

			if err != nil {
				log.Printf("[%v] (!) Orders gateway (OrdersGateway::FindUnprocessed): %s", time.Now(), err.Error())
			}
		}
	}

	return result
}

func (gateway *OrdersGateway) Update(order *Order) bool {
	json, _ := json.Marshal(order.Data)

	if order.CreatedAt.IsZero() {
		order.CreatedAt = time.Now()
	}

	if order.OrderId == nil || order.UserId == nil {
		order.Status = StatusError

		sql := fmt.Sprintf(
			`UPDATE "order"
			SET
				status = '%s',
				pay_status = '%s',
				external_notice = '%s'
			WHERE id = %d`,
			order.Status,
			order.PayStatus,
			ptrToString(order.ExternalNotice),
			order.Id,
		)

		_, err := gateway.db.Exec(sql)

		if err != nil {
			log.Printf("[%v] (!) Orders gateway (OrdersGateway::Update): %s", time.Now(), err.Error())
		}

		return false
	}

	sql := fmt.Sprintf(
		`UPDATE "order"
			SET
				export_data = '%s',
				order_id = %d,
				order_num = '%s',
				outer_user_id = %d,
				status = '%s',
				pay_status = '%s',
				external_notice = '%s',
				product_name = '%s',
				created_at = '%s',
				given_at = '%s',
				next_pay_at = '%s',
				fine_days = %d,
				percent = %f,
				max_amount = %f,
				paid_total = %f,
				total_debt = %f,
				body_debt = %f,
				percents_debt = %f,
				fine_debt = %f
			WHERE id = %d`,
		json,
		ptrToInt(order.OrderId),
		ptrToString(order.OrderNum),
		ptrToInt(order.UserId),
		order.Status,
		order.PayStatus,
		ptrToString(order.ExternalNotice),
		ptrToString(order.ProductName),
		order.CreatedAt.Format("2006-01-02 15:04:05"),
		order.GivenAt.Format("2006-01-02 15:04:05"),
		order.NextPayAt.Format("2006-01-02 15:04:05"),
		ptrToInt(order.FineDays),
		ptrToFloat(order.Percent),
		ptrToFloat(order.MaxAmount),
		ptrToFloat(order.PaidTotal),
		ptrToFloat(order.TotalDebt),
		ptrToFloat(order.BodyDebt),
		ptrToFloat(order.PercentsDebt),
		ptrToFloat(order.FineDebt),
		order.Id,
	)

	log.Printf("[%v] SQL: %s", sql)

	_, err := gateway.db.Exec(sql)

	if err != nil {
		log.Printf("[%v] (!) Orders gateway (OrdersGateway::Update): %s", time.Now(), err.Error())
		return false
	}

	return true
}

func (gateway *OrdersGateway) fetch(query sq.SelectBuilder) *Order {
	orders := []*Order{}
	sql, params, _ := query.ToSql()
	err := gateway.db.Select(&orders, sql, params...)

	if err != nil {
		log.Printf("[%v] (!) Orders gateway: %s", time.Now(), err.Error())
		return nil
	}

	if len(orders) == 0 {
		return nil
	}

	if orders[0].ClientId == 0 {
		orders[0].ClientId = 1
	}

	return orders[0]
}

func ptrToInt(i *int) int {
	if i == nil {
		return 0
	}

	return *i
}

func ptrToFloat(f *float64) float64 {
	if f == nil {
		return 0.0
	}

	return *f
}

func ptrToString(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}
