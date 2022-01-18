package client

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/superoo7/go-gecko/format"

	"api.jacarandapp.com/src/coingecko/types"
)

const waitOnErrors = 60
const baseURL = "https://api.coingecko.com/api/v3"

func CoinInfoDBCreate(coin types.CoinsID) types.CoinInfoDB {

	var coinInfoToWrite types.CoinInfoDB

	coinInfoToWrite.ID = coin.ID
	coinInfoToWrite.Symbol = coin.Symbol
	coinInfoToWrite.Name = coin.Name

	coinInfoToWrite.BlockTimeInMin = coin.BlockTimeInMin
	coinInfoToWrite.Categories = coin.Categories
	coinInfoToWrite.CountryOrigin = coin.CountryOrigin
	coinInfoToWrite.GenesisDate = coin.GenesisDate
	coinInfoToWrite.MarketCapRank = coin.MarketCapRank
	coinInfoToWrite.CoinGeckoRank = coin.CoinGeckoRank
	coinInfoToWrite.CoinGeckoScore = coin.CoinGeckoScore
	coinInfoToWrite.DeveloperScore = coin.DeveloperScore
	coinInfoToWrite.CommunityScore = coin.CommunityScore
	coinInfoToWrite.LiquidityScore = coin.LiquidityScore
	coinInfoToWrite.PublicInterestScore = coin.PublicInterestScore
	coinInfoToWrite.LastUpdated = coin.LastUpdated
	coinInfoToWrite.AlexaRank = coin.PublicInterestStats.AlexaRank
	coinInfoToWrite.BingMatches = coin.PublicInterestStats.BingMatches

	for key, value := range coin.Localization {
		coinInfoToWrite.LocalizationKey = append(coinInfoToWrite.LocalizationKey, key)
		coinInfoToWrite.LocalizationValue = append(coinInfoToWrite.LocalizationValue, value)
	}

	for key, value := range coin.Description {
		coinInfoToWrite.DescriptionKey = append(coinInfoToWrite.DescriptionKey, key)
		coinInfoToWrite.DescriptionValue = append(coinInfoToWrite.DescriptionValue, value)
	}

	coinInfoToWrite.CommunityFacebookLikes = coin.CommunityData.FacebookLikes
	coinInfoToWrite.CommunityTwitterFollowers = coin.CommunityData.TwitterFollowers
	coinInfoToWrite.CommunityRedditAveragePosts48h = coin.CommunityData.RedditAveragePosts48h
	coinInfoToWrite.CommunityRedditAverageComments48h = coin.CommunityData.RedditAverageComments48h
	coinInfoToWrite.CommunityRedditSubscribers = coin.CommunityData.RedditSubscribers
	//coinInfoToWrite.CommunityRedditAccountsActive48h = coin.CommunityData.RedditAccountsActive48h
	coinInfoToWrite.CommunityTelegramChannelUserCount = coin.CommunityData.TelegramChannelUserCount

	coinInfoToWrite.DevForks = coin.DeveloperData.Forks
	coinInfoToWrite.DevStars = coin.DeveloperData.Stars
	coinInfoToWrite.DevSubscribers = coin.DeveloperData.Subscribers
	coinInfoToWrite.DevTotalIssues = coin.DeveloperData.TotalIssues
	coinInfoToWrite.DevClosedIssues = coin.DeveloperData.ClosedIssues
	coinInfoToWrite.DevPRMerged = coin.DeveloperData.PRMerged
	coinInfoToWrite.DevPRContributors = coin.DeveloperData.PRContributors
	coinInfoToWrite.DevCommitsCount4Weeks = coin.DeveloperData.CommitsCount4Weeks

	for k, v := range coin.Links {
		switch c := v.(type) {
		case []string:
			switch k {
			case "homepage":
				coinInfoToWrite.LinksHomepage = c
			case "blockchain_site":
				coinInfoToWrite.LinksBlockchainSite = c
			case "official_forum_url":
				coinInfoToWrite.LinksOfficialForumUrl = c
			case "chat_url":
				coinInfoToWrite.LinksChatUrl = c
			case "announcement_url":
				coinInfoToWrite.LinksAnnouncementUrl = c
			}
		case string:
			switch k {
			case "twitter_screen_name":
				coinInfoToWrite.LinksTwitterScreenName = c
			case "facebook_username":
				coinInfoToWrite.LinksFacebookUsername = c
			case "telegram_channel_identifier":
				coinInfoToWrite.LinksTelegramChannelId = c
			case "subreddit_url":
				coinInfoToWrite.LinksSubredditUrl = c
			}
		case int:
			if k == "bitcointalk_thread_identifier" {
				coinInfoToWrite.LinksBitcointalkThreadIdentifier = c
			}
		default:

		}
	}

	// // coinInfoToWrite.LinksReposGithub
	// // coinInfoToWrite.LinksReposBitbucket

	coinInfoToWrite.ImageThumb = coin.Image.Thumb
	coinInfoToWrite.ImageSmall = coin.Image.Small
	coinInfoToWrite.ImageLarge = coin.Image.Large

	return coinInfoToWrite
}

func errManagment() {
	recoverInfo := recover()
	if recoverInfo != nil {
		fmt.Println(recoverInfo)

		fmt.Println("Waiting 60sec before continue with the requests")
		time.Sleep(time.Duration(time.Second * waitOnErrors))
	}
}

/*******************************************************/

// Client struct
type Client struct {
	httpClient *http.Client
}

// NewClient create new client object
func NewClient(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{httpClient: httpClient}
}

// helper
// doReq HTTP client
func doReq(req *http.Request, client *http.Client) ([]byte, error) {
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if 200 != resp.StatusCode {
		return nil, fmt.Errorf("%s", body)
	}
	return body, nil
}

// MakeReq HTTP request helper
func (c *Client) MakeReq(url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)

	if err != nil {
		return nil, err
	}
	resp, err := doReq(req, c.httpClient)
	if err != nil {
		return nil, err
	}
	return resp, err
}

// API

// Ping /ping endpoint
func (c *Client) Ping() (*types.Ping, error) {
	url := fmt.Sprintf("%s/ping", baseURL)
	resp, err := c.MakeReq(url)
	if err != nil {
		return nil, err
	}
	var data *types.Ping
	err = json.Unmarshal(resp, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// SimpleSinglePrice /simple/price  Single ID and Currency (ids, vs_currency)
func (c *Client) SimpleSinglePrice(id string, vsCurrency string) (*types.SimpleSinglePrice, error) {
	idParam := []string{strings.ToLower(id)}
	vcParam := []string{strings.ToLower(vsCurrency)}

	t, err := c.SimplePrice(idParam, vcParam)
	if err != nil {
		return nil, err
	}
	curr := (*t)[id]
	if len(curr) == 0 {
		return nil, fmt.Errorf("id or vsCurrency not existed")
	}
	data := &types.SimpleSinglePrice{ID: id, Currency: vsCurrency, MarketPrice: curr[vsCurrency]}
	return data, nil
}

// SimplePrice /simple/price Multiple ID and Currency (ids, vs_currencies)
func (c *Client) SimplePrice(ids []string, vsCurrencies []string) (*map[string]map[string]float32, error) {
	params := url.Values{}
	idsParam := strings.Join(ids[:], ",")
	vsCurrenciesParam := strings.Join(vsCurrencies[:], ",")

	params.Add("ids", idsParam)
	params.Add("vs_currencies", vsCurrenciesParam)

	url := fmt.Sprintf("%s/simple/price?%s", baseURL, params.Encode())
	resp, err := c.MakeReq(url)
	if err != nil {
		return nil, err
	}

	t := make(map[string]map[string]float32)
	err = json.Unmarshal(resp, &t)
	if err != nil {
		return nil, err
	}

	return &t, nil
}

// SimpleSupportedVSCurrencies /simple/supported_vs_currencies
func (c *Client) SimpleSupportedVSCurrencies() (*types.SimpleSupportedVSCurrencies, error) {
	url := fmt.Sprintf("%s/simple/supported_vs_currencies", baseURL)
	resp, err := c.MakeReq(url)
	if err != nil {
		return nil, err
	}
	var data *types.SimpleSupportedVSCurrencies
	err = json.Unmarshal(resp, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// CoinsList /coins/list
func (c *Client) CoinsList(platforms bool) (*types.CoinList, error) {
	var url string
	if platforms {
		url = fmt.Sprintf("%s/coins/list?include_platform=true", baseURL)
	} else {
		url = fmt.Sprintf("%s/coins/list", baseURL)
	}
	resp, err := c.MakeReq(url)
	if err != nil {
		return nil, err
	}

	var data *types.CoinList
	err = json.Unmarshal(resp, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// CoinsMarket /coins/market
func (c *Client) CoinsMarket(vsCurrency string, ids []string, order string, perPage int, page int, sparkline bool, priceChangePercentage []string) (*types.CoinsMarket, error) {
	if len(vsCurrency) == 0 {
		return nil, fmt.Errorf("vs_currency is required")
	}
	params := url.Values{}
	// vs_currency
	params.Add("vs_currency", vsCurrency)
	// order
	if len(order) == 0 {
		order = types.OrderTypeObject.MarketCapDesc
	}
	params.Add("order", order)
	// ids
	if len(ids) != 0 {
		idsParam := strings.Join(ids[:], ",")
		params.Add("ids", idsParam)
	}
	// per_page
	if perPage <= 0 || perPage > 250 {
		perPage = 100
	}
	params.Add("per_page", format.Int2String(perPage))
	params.Add("page", format.Int2String(page))
	// sparkline
	params.Add("sparkline", format.Bool2String(sparkline))
	// price_change_percentage
	if len(priceChangePercentage) != 0 {
		priceChangePercentageParam := strings.Join(priceChangePercentage[:], ",")
		params.Add("price_change_percentage", priceChangePercentageParam)
	}
	url := fmt.Sprintf("%s/coins/markets?%s", baseURL, params.Encode())
	resp, err := c.MakeReq(url)
	if err != nil {
		return nil, err
	}
	var data *types.CoinsMarket
	err = json.Unmarshal(resp, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// CoinsID /coins/{id}
func (c *Client) CoinsID(id string, localization bool, tickers bool, marketData bool, communityData bool, developerData bool, sparkline bool) (*types.CoinsID, error) {

	defer errManagment()

	if len(id) == 0 {
		return nil, fmt.Errorf("id is required")
	}
	params := url.Values{}
	params.Add("localization", format.Bool2String(localization))
	params.Add("tickers", format.Bool2String(tickers))
	params.Add("market_data", format.Bool2String(marketData))
	params.Add("community_data", format.Bool2String(communityData))
	params.Add("developer_data", format.Bool2String(developerData))
	params.Add("sparkline", format.Bool2String(sparkline))
	url := fmt.Sprintf("%s/coins/%s?%s", baseURL, id, params.Encode())
	resp, err := c.MakeReq(url)
	if err != nil {
		return nil, err
	}

	var data *types.CoinsID
	err = json.Unmarshal(resp, &data)

	if err != nil {
		return nil, err
	}
	return data, nil
}

// CoinsIDTickers /coins/{id}/tickers
func (c *Client) CoinsIDTickers(id string, page int) (*types.CoinsIDTickers, error) {
	if len(id) == 0 {
		return nil, fmt.Errorf("id is required")
	}
	params := url.Values{}
	if page > 0 {
		params.Add("page", format.Int2String(page))
	}
	url := fmt.Sprintf("%s/coins/%s/tickers?%s", baseURL, id, params.Encode())
	resp, err := c.MakeReq(url)
	if err != nil {
		return nil, err
	}
	var data *types.CoinsIDTickers
	err = json.Unmarshal(resp, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// CoinsIDHistory /coins/{id}/history?date={date}&localization=false
func (c *Client) CoinsIDHistory(id string, date string, localization bool) (*types.CoinsIDHistory, error) {
	if len(id) == 0 || len(date) == 0 {
		return nil, fmt.Errorf("id and date is required")
	}
	params := url.Values{}
	params.Add("date", date)
	params.Add("localization", format.Bool2String(localization))

	url := fmt.Sprintf("%s/coins/%s/history?%s", baseURL, id, params.Encode())
	resp, err := c.MakeReq(url)
	if err != nil {
		return nil, err
	}
	var data *types.CoinsIDHistory
	err = json.Unmarshal(resp, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// CoinsIDMarketChart /coins/{id}/market_chart?vs_currency={usd, eur, jpy, etc.}&days={1,14,30,max}
func (c *Client) CoinsIDMarketChart(id string, vs_currency string, days string) (*types.CoinsIDMarketChart, error) {
	if len(id) == 0 || len(vs_currency) == 0 || len(days) == 0 {
		return nil, fmt.Errorf("id, vs_currency, and days is required")
	}

	params := url.Values{}
	params.Add("vs_currency", vs_currency)
	params.Add("days", days)

	url := fmt.Sprintf("%s/coins/%s/market_chart?%s", baseURL, id, params.Encode())
	resp, err := c.MakeReq(url)
	if err != nil {
		return nil, err
	}

	m := types.CoinsIDMarketChart{}
	err = json.Unmarshal(resp, &m)
	if err != nil {
		return &m, err
	}

	return &m, nil
}

// CoinsIDStatusUpdates

// CoinsIDContractAddress https://api.coingecko.com/api/v3/coins/{id}/contract/{contract_address}
// func CoinsIDContractAddress(id string, address string) (nil, error) {
// 	url := fmt.Sprintf("%s/coins/%s/contract/%s", baseURL, id, address)
// 	resp, err := request.MakeReq(url)
// 	if err != nil {
// 		return nil, err
// 	}
// }

// EventsCountries https://api.coingecko.com/api/v3/events/countries
func (c *Client) EventsCountries() ([]types.EventCountryItem, error) {
	url := fmt.Sprintf("%s/events/countries", baseURL)
	resp, err := c.MakeReq(url)
	if err != nil {
		return nil, err
	}
	var data *types.EventsCountries
	err = json.Unmarshal(resp, &data)
	if err != nil {
		return nil, err
	}
	return data.Data, nil

}

// EventsTypes https://api.coingecko.com/api/v3/events/types
func (c *Client) EventsTypes() (*types.EventsTypes, error) {
	url := fmt.Sprintf("%s/events/types", baseURL)
	resp, err := c.MakeReq(url)
	if err != nil {
		return nil, err
	}
	var data *types.EventsTypes
	err = json.Unmarshal(resp, &data)
	if err != nil {
		return nil, err
	}
	return data, nil

}

// ExchangeRates https://api.coingecko.com/api/v3/exchange_rates
func (c *Client) ExchangeRates() (*types.ExchangeRatesItem, error) {
	url := fmt.Sprintf("%s/exchange_rates", baseURL)
	resp, err := c.MakeReq(url)
	if err != nil {
		return nil, err
	}
	var data *types.ExchangeRatesResponse
	err = json.Unmarshal(resp, &data)
	if err != nil {
		return nil, err
	}
	return &data.Rates, nil
}

// Global https://api.coingecko.com/api/v3/global
func (c *Client) Global() (*types.Global, error) {
	url := fmt.Sprintf("%s/global", baseURL)
	resp, err := c.MakeReq(url)
	if err != nil {
		return nil, err
	}
	var data *types.GlobalResponse
	err = json.Unmarshal(resp, &data)
	if err != nil {
		return nil, err
	}
	return &data.Data, nil
}
