package shipment

import (
	"encoding/json"
	"log"

	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/uuuid"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	"github.com/pkg/errors"
)

// Insert handles "insert" events.
func Insert(collection *mongo.Collection, event *model.Event) *model.KafkaResponse {
	ship := &Shipment{}
	err := json.Unmarshal(event.Data, ship)
	if err != nil {
		err = errors.Wrap(err, "Insert: Error while unmarshalling Event-data")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			UUID:          event.TimeUUID,
		}
	}

	if ship.ItemID == (uuuid.UUID{}) {
		err = errors.New("missing ItemID")
		err = errors.Wrap(err, "Insert")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			UUID:          event.TimeUUID,
		}
	}

	insertResult, err := collection.InsertOne(ship)
	if err != nil {
		err = errors.Wrap(err, "Insert: Error Inserting shipment into Mongo")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     DatabaseError,
			UUID:          event.TimeUUID,
		}
	}
	insertedID, assertOK := insertResult.InsertedID.(objectid.ObjectID)
	if !assertOK {
		err = errors.New("error asserting InsertedID from InsertResult to ObjectID")
		err = errors.Wrap(err, "Insert")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			UUID:          event.TimeUUID,
		}
	}

	ship.ID = insertedID
	result, err := json.Marshal(ship)
	if err != nil {
		err = errors.Wrap(err, "Insert: Error marshalling Shipment Insert-result")
		log.Println(err)
		return &model.KafkaResponse{
			AggregateID:   event.AggregateID,
			CorrelationID: event.CorrelationID,
			Error:         err.Error(),
			ErrorCode:     InternalError,
			UUID:          event.TimeUUID,
		}
	}

	return &model.KafkaResponse{
		AggregateID:   event.AggregateID,
		CorrelationID: event.CorrelationID,
		Result:        result,
		UUID:          event.TimeUUID,
	}
}
