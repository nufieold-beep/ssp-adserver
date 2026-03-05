package geo

import (
	"log"
	"net"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/oschwald/geoip2-golang"
)

var cityDB *geoip2.Reader
var asnDB *geoip2.Reader

// Result holds the geo + carrier data from a MaxMind lookup.
type Result struct {
	Lat       float64
	Lon       float64
	Country   string // ISO 3166-1 alpha-3 (e.g. "USA")
	Region    string // ISO 3166-2 subdivision code (e.g. "PA")
	Metro     string // DMA/metro code
	City      string
	Zip       string
	Accuracy  int
	IPService int // 3 = MaxMind
	Carrier   string
}

// Init loads MaxMind GeoLite2 databases from dataDir.
// Silently skips if files are not present.
func Init(dataDir string) {
	cityPath := filepath.Join(dataDir, "GeoLite2-City.mmdb")
	var err error
	cityDB, err = geoip2.Open(cityPath)
	if err != nil {
		log.Printf("[geo] GeoLite2-City not loaded (%s): %v", cityPath, err)
		cityDB = nil
	} else {
		log.Printf("[geo] GeoLite2-City loaded from %s", cityPath)
	}

	asnPath := filepath.Join(dataDir, "GeoLite2-ASN.mmdb")
	asnDB, err = geoip2.Open(asnPath)
	if err != nil {
		log.Printf("[geo] GeoLite2-ASN not loaded (%s): %v", asnPath, err)
		asnDB = nil
	} else {
		log.Printf("[geo] GeoLite2-ASN loaded from %s", asnPath)
	}
}

// Lookup returns geo + carrier data for an IP address.
// Returns nil if MaxMind databases are not loaded or IP is invalid.
func Lookup(ipStr string) *Result {
	if cityDB == nil {
		return nil
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return nil
	}

	record, err := cityDB.City(ip)
	if err != nil {
		return nil
	}

	result := &Result{
		Lat:       record.Location.Latitude,
		Lon:       record.Location.Longitude,
		City:      record.City.Names["en"],
		Zip:       record.Postal.Code,
		Accuracy:  int(record.Location.AccuracyRadius),
		IPService: 3, // MaxMind
	}

	if record.Country.IsoCode != "" {
		result.Country = toAlpha3(record.Country.IsoCode)
	}
	if len(record.Subdivisions) > 0 {
		result.Region = record.Subdivisions[0].IsoCode
	}
	if record.Location.MetroCode > 0 {
		result.Metro = strconv.Itoa(int(record.Location.MetroCode))
	}

	// Carrier / ISP from ASN database
	if asnDB != nil {
		asnRecord, err := asnDB.ASN(ip)
		if err == nil {
			result.Carrier = asnRecord.AutonomousSystemOrganization
		}
	}

	return result
}

// Close releases the MaxMind database handles.
func Close() {
	if cityDB != nil {
		cityDB.Close()
	}
	if asnDB != nil {
		asnDB.Close()
	}
}

// DetectIFAType returns the IFA type based on UA / device info.
func DetectIFAType(ua, make_, os_ string) string {
	uaLower := strings.ToLower(ua)
	makeLower := strings.ToLower(make_)
	osLower := strings.ToLower(os_)

	switch {
	case strings.Contains(uaLower, "tizen") || strings.Contains(makeLower, "samsung"):
		return "tifa"
	case strings.Contains(uaLower, "aft") || strings.Contains(makeLower, "amazon"):
		return "afai"
	case strings.Contains(uaLower, "roku") || strings.Contains(makeLower, "roku"):
		return "rida"
	case strings.Contains(uaLower, "webos") || strings.Contains(makeLower, "lg"):
		return "lgudid"
	case strings.Contains(osLower, "tvos") || strings.Contains(makeLower, "apple"):
		return "idfa"
	case strings.Contains(uaLower, "vizio"):
		return "vtifa"
	default:
		return "gaid" // Android / generic
	}
}

// toAlpha3 converts ISO 3166-1 alpha-2 to alpha-3.
func toAlpha3(code string) string {
	if v, ok := alpha2to3[strings.ToUpper(code)]; ok {
		return v
	}
	return code
}

var alpha2to3 = map[string]string{
	"AF": "AFG", "AX": "ALA", "AL": "ALB", "DZ": "DZA", "AS": "ASM",
	"AD": "AND", "AO": "AGO", "AI": "AIA", "AQ": "ATA", "AG": "ATG",
	"AR": "ARG", "AM": "ARM", "AW": "ABW", "AU": "AUS", "AT": "AUT",
	"AZ": "AZE", "BS": "BHS", "BH": "BHR", "BD": "BGD", "BB": "BRB",
	"BY": "BLR", "BE": "BEL", "BZ": "BLZ", "BJ": "BEN", "BM": "BMU",
	"BT": "BTN", "BO": "BOL", "BA": "BIH", "BW": "BWA", "BV": "BVT",
	"BR": "BRA", "IO": "IOT", "BN": "BRN", "BG": "BGR", "BF": "BFA",
	"BI": "BDI", "KH": "KHM", "CM": "CMR", "CA": "CAN", "CV": "CPV",
	"KY": "CYM", "CF": "CAF", "TD": "TCD", "CL": "CHL", "CN": "CHN",
	"CX": "CXR", "CC": "CCK", "CO": "COL", "KM": "COM", "CG": "COG",
	"CD": "COD", "CK": "COK", "CR": "CRI", "CI": "CIV", "HR": "HRV",
	"CU": "CUB", "CY": "CYP", "CZ": "CZE", "DK": "DNK", "DJ": "DJI",
	"DM": "DMA", "DO": "DOM", "EC": "ECU", "EG": "EGY", "SV": "SLV",
	"GQ": "GNQ", "ER": "ERI", "EE": "EST", "ET": "ETH", "FK": "FLK",
	"FO": "FRO", "FJ": "FJI", "FI": "FIN", "FR": "FRA", "GF": "GUF",
	"PF": "PYF", "TF": "ATF", "GA": "GAB", "GM": "GMB", "GE": "GEO",
	"DE": "DEU", "GH": "GHA", "GI": "GIB", "GR": "GRC", "GL": "GRL",
	"GD": "GRD", "GP": "GLP", "GU": "GUM", "GT": "GTM", "GG": "GGY",
	"GN": "GIN", "GW": "GNB", "GY": "GUY", "HT": "HTI", "HM": "HMD",
	"VA": "VAT", "HN": "HND", "HK": "HKG", "HU": "HUN", "IS": "ISL",
	"IN": "IND", "ID": "IDN", "IR": "IRN", "IQ": "IRQ", "IE": "IRL",
	"IM": "IMN", "IL": "ISR", "IT": "ITA", "JM": "JAM", "JP": "JPN",
	"JE": "JEY", "JO": "JOR", "KZ": "KAZ", "KE": "KEN", "KI": "KIR",
	"KP": "PRK", "KR": "KOR", "KW": "KWT", "KG": "KGZ", "LA": "LAO",
	"LV": "LVA", "LB": "LBN", "LS": "LSO", "LR": "LBR", "LY": "LBY",
	"LI": "LIE", "LT": "LTU", "LU": "LUX", "MO": "MAC", "MK": "MKD",
	"MG": "MDG", "MW": "MWI", "MY": "MYS", "MV": "MDV", "ML": "MLI",
	"MT": "MLT", "MH": "MHL", "MQ": "MTQ", "MR": "MRT", "MU": "MUS",
	"YT": "MYT", "MX": "MEX", "FM": "FSM", "MD": "MDA", "MC": "MCO",
	"MN": "MNG", "ME": "MNE", "MS": "MSR", "MA": "MAR", "MZ": "MOZ",
	"MM": "MMR", "NA": "NAM", "NR": "NRU", "NP": "NPL", "NL": "NLD",
	"NC": "NCL", "NZ": "NZL", "NI": "NIC", "NE": "NER", "NG": "NGA",
	"NU": "NIU", "NF": "NFK", "MP": "MNP", "NO": "NOR", "OM": "OMN",
	"PK": "PAK", "PW": "PLW", "PS": "PSE", "PA": "PAN", "PG": "PNG",
	"PY": "PRY", "PE": "PER", "PH": "PHL", "PN": "PCN", "PL": "POL",
	"PT": "PRT", "PR": "PRI", "QA": "QAT", "RE": "REU", "RO": "ROU",
	"RU": "RUS", "RW": "RWA", "BL": "BLM", "SH": "SHN", "KN": "KNA",
	"LC": "LCA", "MF": "MAF", "PM": "SPM", "VC": "VCT", "WS": "WSM",
	"SM": "SMR", "ST": "STP", "SA": "SAU", "SN": "SEN", "RS": "SRB",
	"SC": "SYC", "SL": "SLE", "SG": "SGP", "SK": "SVK", "SI": "SVN",
	"SB": "SLB", "SO": "SOM", "ZA": "ZAF", "GS": "SGS", "ES": "ESP",
	"LK": "LKA", "SD": "SDN", "SR": "SUR", "SJ": "SJM", "SZ": "SWZ",
	"SE": "SWE", "CH": "CHE", "SY": "SYR", "TW": "TWN", "TJ": "TJK",
	"TZ": "TZA", "TH": "THA", "TL": "TLS", "TG": "TGO", "TK": "TKL",
	"TO": "TON", "TT": "TTO", "TN": "TUN", "TR": "TUR", "TM": "TKM",
	"TC": "TCA", "TV": "TUV", "UG": "UGA", "UA": "UKR", "AE": "ARE",
	"GB": "GBR", "US": "USA", "UM": "UMI", "UY": "URY", "UZ": "UZB",
	"VU": "VUT", "VE": "VEN", "VN": "VNM", "VG": "VGB", "VI": "VIR",
	"WF": "WLF", "EH": "ESH", "YE": "YEM", "ZM": "ZMB", "ZW": "ZWE",
}
