package geo

import (
	"log"
	"net"
	"strconv"
	"strings"
	"sync"

	"ssp/internal/openrtb"

	"github.com/oschwald/geoip2-golang"
)

// GeoResult holds the geo + carrier data from MaxMind lookup.
type GeoResult struct {
	Lat       float64
	Lon       float64
	Country   string // ISO-3166-1 alpha-3 (e.g. "USA")
	Region    string // ISO subdivision code (e.g. "PA")
	Metro     string // DMA/metro code
	City      string
	Zip       string
	Accuracy  int
	IPService int // 3 = MaxMind
	Carrier   string
}

var (
	cityDB *geoip2.Reader
	asnDB  *geoip2.Reader
	once   sync.Once
	ready  bool
)

// Init opens the MaxMind GeoIP2 database files.
// If a file is missing or unreadable the lookup gracefully degrades.
func Init(cityPath, asnPath string) {
	once.Do(func() {
		if cityPath != "" {
			db, err := geoip2.Open(cityPath)
			if err != nil {
				log.Printf("[geo] Could not open city DB %s: %v (geo enrichment disabled)", cityPath, err)
			} else {
				cityDB = db
				log.Printf("[geo] Loaded city DB: %s", cityPath)
			}
		}
		if asnPath != "" {
			db, err := geoip2.Open(asnPath)
			if err != nil {
				log.Printf("[geo] Could not open ASN DB %s: %v (carrier enrichment disabled)", asnPath, err)
			} else {
				asnDB = db
				log.Printf("[geo] Loaded ASN DB: %s", asnPath)
			}
		}
		ready = cityDB != nil || asnDB != nil
	})
}

// Lookup performs a MaxMind geo + ASN lookup for the given IP address.
// Returns nil if databases are unavailable or the IP is invalid.
func Lookup(ipStr string) *GeoResult {
	if !ready || ipStr == "" {
		return nil
	}

	ip := net.ParseIP(ipStr)
	if ip == nil {
		return nil
	}

	result := &GeoResult{IPService: 3}

	if cityDB != nil {
		record, err := cityDB.City(ip)
		if err == nil {
			result.Lat = record.Location.Latitude
			result.Lon = record.Location.Longitude
			result.Accuracy = int(record.Location.AccuracyRadius)
			if record.Country.IsoCode != "" {
				result.Country = CountryISO3(record.Country.IsoCode)
			}
			if len(record.Subdivisions) > 0 {
				result.Region = record.Subdivisions[0].IsoCode
			}
			if name, ok := record.City.Names["en"]; ok {
				result.City = name
			}
			if record.Postal.Code != "" {
				result.Zip = record.Postal.Code
			}
			metro := int(record.Location.MetroCode)
			if metro > 0 {
				result.Metro = strconv.Itoa(metro)
			}
		}
	}

	if asnDB != nil {
		record, err := asnDB.ASN(ip)
		if err == nil && record.AutonomousSystemOrganization != "" {
			result.Carrier = record.AutonomousSystemOrganization
		}
	}

	return result
}

// EnrichRequest fills in missing Device.Geo and Device.Carrier fields
// from MaxMind. Already-populated fields from query params are preserved.
func EnrichRequest(req *openrtb.BidRequest) {
	if req.Device.IP == "" {
		return
	}

	geo := Lookup(req.Device.IP)
	if geo == nil {
		return
	}

	// Ensure Geo struct exists
	if req.Device.Geo == nil {
		req.Device.Geo = &openrtb.Geo{Type: 2}
	}
	g := req.Device.Geo

	// Only fill in missing fields — publisher-supplied data takes priority
	if g.Lat == 0 && g.Lon == 0 {
		g.Lat = geo.Lat
		g.Lon = geo.Lon
	}
	if g.Country == "" {
		g.Country = geo.Country
	}
	if g.Region == "" {
		g.Region = geo.Region
	}
	if g.Metro == "" {
		g.Metro = geo.Metro
	}
	if g.City == "" {
		g.City = geo.City
	}
	if g.Zip == "" {
		g.Zip = geo.Zip
	}
	if g.Accuracy == 0 && geo.Accuracy > 0 {
		g.Accuracy = geo.Accuracy
	}
	if g.IPService == 0 {
		g.IPService = geo.IPService
	}
	if g.Type == 0 {
		g.Type = 2 // IP-derived
	}

	// Carrier
	if req.Device.Carrier == "" && geo.Carrier != "" {
		req.Device.Carrier = geo.Carrier
	}
}

// Close releases the MaxMind database readers.
func Close() {
	if cityDB != nil {
		cityDB.Close()
	}
	if asnDB != nil {
		asnDB.Close()
	}
}

// CountryISO3 converts ISO-3166-1 alpha-2 codes to alpha-3.
// Falls back to the original code if no mapping exists.
func CountryISO3(iso2 string) string {
	if v, ok := iso2to3[strings.ToUpper(iso2)]; ok {
		return v
	}
	return iso2
}

var iso2to3 = map[string]string{
	"AF": "AFG", "AL": "ALB", "DZ": "DZA", "AD": "AND", "AO": "AGO",
	"AG": "ATG", "AR": "ARG", "AM": "ARM", "AU": "AUS", "AT": "AUT",
	"AZ": "AZE", "BS": "BHS", "BH": "BHR", "BD": "BGD", "BB": "BRB",
	"BY": "BLR", "BE": "BEL", "BZ": "BLZ", "BJ": "BEN", "BT": "BTN",
	"BO": "BOL", "BA": "BIH", "BW": "BWA", "BR": "BRA", "BN": "BRN",
	"BG": "BGR", "BF": "BFA", "BI": "BDI", "KH": "KHM", "CM": "CMR",
	"CA": "CAN", "CV": "CPV", "CF": "CAF", "TD": "TCD", "CL": "CHL",
	"CN": "CHN", "CO": "COL", "KM": "COM", "CG": "COG", "CD": "COD",
	"CR": "CRI", "CI": "CIV", "HR": "HRV", "CU": "CUB", "CY": "CYP",
	"CZ": "CZE", "DK": "DNK", "DJ": "DJI", "DM": "DMA", "DO": "DOM",
	"EC": "ECU", "EG": "EGY", "SV": "SLV", "GQ": "GNQ", "ER": "ERI",
	"EE": "EST", "ET": "ETH", "FJ": "FJI", "FI": "FIN", "FR": "FRA",
	"GA": "GAB", "GM": "GMB", "GE": "GEO", "DE": "DEU", "GH": "GHA",
	"GR": "GRC", "GD": "GRD", "GT": "GTM", "GN": "GIN", "GW": "GNB",
	"GY": "GUY", "HT": "HTI", "HN": "HND", "HU": "HUN", "IS": "ISL",
	"IN": "IND", "ID": "IDN", "IR": "IRN", "IQ": "IRQ", "IE": "IRL",
	"IL": "ISR", "IT": "ITA", "JM": "JAM", "JP": "JPN", "JO": "JOR",
	"KZ": "KAZ", "KE": "KEN", "KI": "KIR", "KP": "PRK", "KR": "KOR",
	"KW": "KWT", "KG": "KGZ", "LA": "LAO", "LV": "LVA", "LB": "LBN",
	"LS": "LSO", "LR": "LBR", "LY": "LBY", "LI": "LIE", "LT": "LTU",
	"LU": "LUX", "MK": "MKD", "MG": "MDG", "MW": "MWI", "MY": "MYS",
	"MV": "MDV", "ML": "MLI", "MT": "MLT", "MH": "MHL", "MR": "MRT",
	"MU": "MUS", "MX": "MEX", "FM": "FSM", "MD": "MDA", "MC": "MCO",
	"MN": "MNG", "ME": "MNE", "MA": "MAR", "MZ": "MOZ", "MM": "MMR",
	"NA": "NAM", "NR": "NRU", "NP": "NPL", "NL": "NLD", "NZ": "NZL",
	"NI": "NIC", "NE": "NER", "NG": "NGA", "NO": "NOR", "OM": "OMN",
	"PK": "PAK", "PW": "PLW", "PA": "PAN", "PG": "PNG", "PY": "PRY",
	"PE": "PER", "PH": "PHL", "PL": "POL", "PT": "PRT", "QA": "QAT",
	"RO": "ROU", "RU": "RUS", "RW": "RWA", "KN": "KNA", "LC": "LCA",
	"VC": "VCT", "WS": "WSM", "SM": "SMR", "ST": "STP", "SA": "SAU",
	"SN": "SEN", "RS": "SRB", "SC": "SYC", "SL": "SLE", "SG": "SGP",
	"SK": "SVK", "SI": "SVN", "SB": "SLB", "SO": "SOM", "ZA": "ZAF",
	"ES": "ESP", "LK": "LKA", "SD": "SDN", "SR": "SUR", "SZ": "SWZ",
	"SE": "SWE", "CH": "CHE", "SY": "SYR", "TW": "TWN", "TJ": "TJK",
	"TZ": "TZA", "TH": "THA", "TL": "TLS", "TG": "TGO", "TO": "TON",
	"TT": "TTO", "TN": "TUN", "TR": "TUR", "TM": "TKM", "TV": "TUV",
	"UG": "UGA", "UA": "UKR", "AE": "ARE", "GB": "GBR", "US": "USA",
	"UY": "URY", "UZ": "UZB", "VU": "VUT", "VE": "VEN", "VN": "VNM",
	"YE": "YEM", "ZM": "ZMB", "ZW": "ZWE",
	"HK": "HKG", "MO": "MAC", "PS": "PSE", "PR": "PRI", "GU": "GUM",
	"VI": "VIR", "AS": "ASM", "MP": "MNP", "CW": "CUW", "SX": "SXM",
	"XK": "XKX", "SS": "SSD",
}
