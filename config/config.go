package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)


const envFilePath = "../../.env"
/// "/srv/applications/broadcast-daemon/.env"

func LoadEnvFile() {
	err := godotenv.Load(envFilePath)
	if err != nil {
		log.Printf("Error loading .env file from path %s: %v", envFilePath, err)
	}
}


func GetEnv(key, fallback string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return fallback
	}
	return value
}
