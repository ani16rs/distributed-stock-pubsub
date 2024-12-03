package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv"
)

// StockUpdate represents a stock update message
type StockUpdate struct {
	StockSymbol string  `json:"stock_symbol"`
	Price       float64 `json:"price"`
	Timestamp   string  `json:"timestamp"`
}

// FetchStockData fetches stock data from Alpha Vantage API
func FetchStockData(symbol, apiKey string) (float64, error) {
	baseURL := "https://www.alphavantage.co/query"
	url := fmt.Sprintf("%s?function=GLOBAL_QUOTE&symbol=%s&apikey=%s", baseURL, symbol, apiKey)
	resp, err := http.Get(url)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch stock data: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("received non-200 response: %v", resp.StatusCode)
	}

	// Parse the response
	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		return 0, fmt.Errorf("failed to parse response: %v", err)
	}

	// Extract the price
	quote, ok := result["Global Quote"].(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("unexpected API response format")
	}

	priceStr, ok := quote["05. price"].(string)
	if !ok {
		return 0, fmt.Errorf("price not found in API response")
	}

	var price float64
	_, err = fmt.Sscanf(priceStr, "%f", &price)
	if err != nil {
		return 0, fmt.Errorf("failed to parse price: %v", err)
	}

	return price, nil
}

// SendStockUpdate sends stock data to the broker
func SendStockUpdate(stock StockUpdate, brokerURL string) error {
	data, err := json.Marshal(stock)
	if err != nil {
		return fmt.Errorf("failed to serialize stock update: %v", err)
	}

	resp, err := http.Post(brokerURL, "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to send stock update: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("broker returned non-200 response: %v", resp.StatusCode)
	}

	return nil
}

func main() {
	// Load environment variables
	err := godotenv.Load(".env")
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	// Retrieve API key and Broker URL from environment
	apiKey := os.Getenv("API_KEY")
	if apiKey == "" {
		log.Fatal("API_KEY is not set in the .env file")
	}

	brokerURL := os.Getenv("BROKER_URL")
	if brokerURL == "" {
		log.Fatal("BROKER_URL is not set in the .env file")
	}

	// Define the stock symbols to monitor
	stocks := []string{"AAPL", "GOOGL", "MSFT"}

	// Fetch and send stock data every 10 seconds
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		for _, symbol := range stocks {
			price, err := FetchStockData(symbol, apiKey)
			if err != nil {
				log.Printf("Error fetching data for %s: %v", symbol, err)
				continue
			}

			stockUpdate := StockUpdate{
				StockSymbol: symbol,
				Price:       price,
				Timestamp:   time.Now().Format(time.RFC3339),
			}

			err = SendStockUpdate(stockUpdate, brokerURL)
			if err != nil {
				log.Printf("Error sending update for %s: %v", symbol, err)
			} else {
				log.Printf("Successfully sent update: %+v", stockUpdate)
			}
		}
	}
}

