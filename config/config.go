package config

const (
	KafkaBroker = "172.30.4.177:9092"
	PgConnStr   = "user=postgres host=172.30.4.177 password=amir24 dbname=test-transaction sslmode=disable"
	BatchSize   = 20
	MaxRetries  = 3
)

var (
	TransactionDailyGroupID     = "transaction_daily_group"
	TransactionUserDailyGroupID = "transaction_user_daily_group"
	Topic                       = "transaction_created" // Kafka topic name
)
