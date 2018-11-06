package test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/agg-shipment-cmd/shipment"
	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/uuuid"
	"github.com/joho/godotenv"
	"github.com/mongodb/mongo-go-driver/bson/objectid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

func Byf(s string, args ...interface{}) {
	By(fmt.Sprintf(s, args...))
}

func TestInventory(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_EVENT_GROUP",

		"KAFKA_CONSUMER_EVENT_TOPIC",
		"KAFKA_CONSUMER_EVENT_QUERY_GROUP",
		"KAFKA_CONSUMER_EVENT_QUERY_TOPIC",

		"KAFKA_PRODUCER_EVENT_TOPIC",
		"KAFKA_PRODUCER_EVENT_QUERY_TOPIC",
		"KAFKA_PRODUCER_RESPONSE_TOPIC",

		"MONGO_HOSTS",
		"MONGO_USERNAME",
		"MONGO_PASSWORD",
		"MONGO_DATABASE",
		"MONGO_CONNECTION_TIMEOUT_MS",
		"MONGO_RESOURCE_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required for testing, but is not set", missingVar)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "ShipmentAggregate Suite")
}

var _ = Describe("ShipmentAggregate", func() {
	var aggregateID int8 = 6
	var (
		kafkaBrokers          []string
		eventsTopic           string
		producerResponseTopic string

		mockShip  *shipment.Shipment
		mockEvent *model.Event
	)
	BeforeSuite(func() {
		kafkaBrokers = *commonutil.ParseHosts(
			os.Getenv("KAFKA_BROKERS"),
		)
		eventsTopic = os.Getenv("KAFKA_PRODUCER_EVENT_TOPIC")
		producerResponseTopic = os.Getenv("KAFKA_PRODUCER_RESPONSE_TOPIC")

		itemID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		deviceID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		rsCustomerID, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())

		mockShip = &shipment.Shipment{
			ItemID:       itemID,
			DateArrived:  time.Now().Unix(),
			DeviceID:     deviceID,
			Lot:          "test-lot",
			Name:         "test-name",
			Origin:       "test-origin",
			Price:        13.4,
			Quantity:     45,
			RSCustomerID: rsCustomerID,
			SalePrice:    12.23,
			SKU:          "test-sku",
			Timestamp:    time.Now().Unix(),
			TotalWeight:  300,
			UPC:          123456789012,
			WasteWeight:  12,
		}
		marshalShip, err := json.Marshal(mockShip)
		Expect(err).ToNot(HaveOccurred())

		cid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		uid, err := uuuid.NewV4()
		Expect(err).ToNot(HaveOccurred())
		timeUUID, err := uuuid.NewV1()
		Expect(err).ToNot(HaveOccurred())
		mockEvent = &model.Event{
			Action:        "insert",
			CorrelationID: cid,
			AggregateID:   aggregateID,
			Data:          marshalShip,
			Timestamp:     time.Now(),
			UserUUID:      uid,
			TimeUUID:      timeUUID,
			Version:       0,
			YearBucket:    2018,
		}
	})

	Describe("Shipment Operations", func() {
		It("should insert record", func(done Done) {
			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "aggship.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.TimeUUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.TimeUUID))

					ship := &shipment.Shipment{}
					err = json.Unmarshal(kr.Result, ship)
					Expect(err).ToNot(HaveOccurred())

					if ship.ItemID == mockShip.ItemID {
						mockShip.ID = ship.ID
						Expect(ship).To(Equal(mockShip))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			c.Consume(context.Background(), handler)

			Byf("Checking if record got inserted into Database")
			aggColl, err := loadAggCollection()
			Expect(err).ToNot(HaveOccurred())
			findResult, err := aggColl.FindOne(mockShip)
			Expect(err).ToNot(HaveOccurred())
			findShip, assertOK := findResult.(*shipment.Shipment)
			Expect(assertOK).To(BeTrue())
			Expect(findShip).To(Equal(mockShip))

			close(done)
		}, 20)

		It("should update record", func(done Done) {
			Byf("Creating update args")
			filterShip := map[string]interface{}{
				"itemID": mockShip.ItemID,
			}
			mockShip.Origin = "new-origin"
			mockShip.ExpiryDate = time.Now().Unix()
			// Remove ObjectID because this is not passed from gateway
			mockID := mockShip.ID
			mockShip.ID = objectid.NilObjectID
			update := map[string]interface{}{
				"filter": filterShip,
				"update": mockShip,
			}
			marshalUpdate, err := json.Marshal(update)
			Expect(err).ToNot(HaveOccurred())
			// Reassign back ID so we can compare easily with database-entry
			mockShip.ID = mockID

			Byf("Creating update MockEvent")
			timeUUID, err := uuuid.NewV1()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.Action = "update"
			mockEvent.Data = marshalUpdate
			mockEvent.Timestamp = time.Now()
			mockEvent.TimeUUID = timeUUID

			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "aggship.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.TimeUUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.TimeUUID))

					result := map[string]int{}
					err = json.Unmarshal(kr.Result, &result)
					Expect(err).ToNot(HaveOccurred())

					if result["matchedCount"] != 0 && result["modifiedCount"] != 0 {
						Expect(result["matchedCount"]).To(Equal(1))
						Expect(result["modifiedCount"]).To(Equal(1))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			c.Consume(context.Background(), handler)

			Byf("Checking if record got inserted into Database")
			aggColl, err := loadAggCollection()
			Expect(err).ToNot(HaveOccurred())
			findResult, err := aggColl.FindOne(mockShip)
			Expect(err).ToNot(HaveOccurred())
			findShip, assertOK := findResult.(*shipment.Shipment)
			Expect(assertOK).To(BeTrue())
			Expect(findShip).To(Equal(mockShip))

			close(done)
		}, 20)

		It("should delete record", func(done Done) {
			Byf("Creating delete args")
			deleteArgs := map[string]interface{}{
				"itemID": mockShip.ItemID,
			}
			marshalDelete, err := json.Marshal(deleteArgs)
			Expect(err).ToNot(HaveOccurred())

			Byf("Creating delete MockEvent")
			timeUUID, err := uuuid.NewV1()
			Expect(err).ToNot(HaveOccurred())
			mockEvent.Action = "delete"
			mockEvent.Data = marshalDelete
			mockEvent.Timestamp = time.Now()
			mockEvent.TimeUUID = timeUUID

			Byf("Producing MockEvent")
			p, err := kafka.NewProducer(&kafka.ProducerConfig{
				KafkaBrokers: kafkaBrokers,
			})
			Expect(err).ToNot(HaveOccurred())
			marshalEvent, err := json.Marshal(mockEvent)
			Expect(err).ToNot(HaveOccurred())
			p.Input() <- kafka.CreateMessage(eventsTopic, marshalEvent)

			// Check if MockEvent was processed correctly
			Byf("Consuming Result")
			c, err := kafka.NewConsumer(&kafka.ConsumerConfig{
				KafkaBrokers: kafkaBrokers,
				GroupName:    "aggship.test.group.1",
				Topics:       []string{producerResponseTopic},
			})
			msgCallback := func(msg *sarama.ConsumerMessage) bool {
				defer GinkgoRecover()
				kr := &model.KafkaResponse{}
				err := json.Unmarshal(msg.Value, kr)
				Expect(err).ToNot(HaveOccurred())

				if kr.UUID == mockEvent.TimeUUID {
					Expect(kr.Error).To(BeEmpty())
					Expect(kr.ErrorCode).To(BeZero())
					Expect(kr.CorrelationID).To(Equal(mockEvent.CorrelationID))
					Expect(kr.UUID).To(Equal(mockEvent.TimeUUID))

					result := map[string]int{}
					err = json.Unmarshal(kr.Result, &result)
					Expect(err).ToNot(HaveOccurred())

					if result["deletedCount"] != 0 {
						Expect(result["deletedCount"]).To(Equal(1))
						return true
					}
				}
				return false
			}

			handler := &msgHandler{msgCallback}
			c.Consume(context.Background(), handler)

			Byf("Checking if record got inserted into Database")
			aggColl, err := loadAggCollection()
			Expect(err).ToNot(HaveOccurred())
			_, err = aggColl.FindOne(mockShip)
			Expect(err).To(HaveOccurred())

			close(done)
		}, 20)
	})
})
