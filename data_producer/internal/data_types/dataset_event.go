package data_types

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type DataSetEvent struct {
	EventTime    time.Time `json:"event_time" parquet:"event_time"`
	EventType    string    `json:"event_type" parquet:"event_type"`
	ProductId    int64     `json:"product_id" parquet:"product_id"`
	CategoryId   string    `json:"category_id" parquet:"category_id"`
	CategoryCode string    `json:"category_code" parquet:"category_code"`
	Brand        string    `json:"brand" parquet:"brand"`
	Price        string    `json:"price" parquet:"price"`
	UserId       int64     `json:"user_id" parquet:"user_id"`
	UserSession  string    `json:"user_session" parquet:"user_session"`
}

type DataSetEventWithRowID struct {
	DataSetEvent
	RowID uuid.UUID `json:"row_id"`
}

func (dse DataSetEvent) WithRowID(rowID uuid.UUID) DataSetEventWithRowID {
	return DataSetEventWithRowID{
		DataSetEvent: dse,
		RowID:        rowID,
	}
}

func (dse *DataSetEventWithRowID) GetKey() ([]byte, error) {
	return []byte(dse.UserSession), nil
}

func (dse *DataSetEventWithRowID) GetValue() ([]byte, error) {
	value, err := json.Marshal(dse)
	if err != nil {
		return nil, err
	}
	return value, nil
}
