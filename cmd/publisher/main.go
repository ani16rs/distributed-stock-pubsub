package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// Constants for Alpha Vantage API
const (
	BASE_URL = "https://www.alphavantage.co/query"
	API_KEY  = "XJ0OFBX1HLQD6OYM"
)

// Broker Endpoint
const (
	BROKER_URL = "http://<BROKER_IP>:8080/update" // Replace <BROKER_IP> with the broker's IP
)

// StockUpdate represents a stock update message
type StockUpdate struct {
	StockSymbol string  `json:"stock_symbol"`
	Price       float64 `json:"price"`
	Timestamp   string  `json:"timestamp"`
}

// FetchStockData fetches stock data from Alpha Vantage API
func FetchStockData(symbol string) (float64, error) {
	url := fmt.Sprintf("%s?function=GLOBAL_QUOTE&symbol=%s&apikey=%s", BASE_URL, symbol, API_KEY)
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
func SendStockUpdate(stock StockUpdate) error {
	data, err := json.Marshal(stock)
	if err != nil {
		return fmt.Errorf("failed to serialize stock update: %v", err)
	}

	resp, err := http.Post(BROKER_URL, "application/json", bytes.NewReader(data))
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
	// Define the stock symbols to monitor
	stocks := []string{"AAPL", "GOOGL", "MSFT"}

	// Fetch and send stock data every 10 seconds
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		for _, symbol := range stocks {
			price, err := FetchStockData(symbol)
			if err != nil {
				log.Printf("Error fetching data for %s: %v", symbol, err)
				continue
			}

			stockUpdate := StockUpdate{
				StockSymbol: symbol,
				Price:       price,
				Timestamp:   time.Now().Format(time.RFC3339),
			}

			err = SendStockUpdate(stockUpdate)
			if err != nil {
				log.Printf("Error sending update for %s: %v", symbol, err)
			} else {
				log.Printf("Successfully sent update: %+v", stockUpdate)
			}
		}
	}
}

