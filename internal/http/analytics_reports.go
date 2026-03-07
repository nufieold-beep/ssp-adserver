package http

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"ssp/internal/monitor"
	"ssp/internal/openrtb"
)

type analyticsState struct {
	decisions     []AdDecision
	supplyNames   map[int]string
	demandNames   map[string]string
	allTimeTotals analyticsAccumulator
	demandTotals  map[string]analyticsAccumulator
	supplyTotals  map[int]analyticsAccumulator
	bundleTotals  map[string]analyticsAccumulator
}

type persistedAnalyticsTotals struct {
	FilledOpportunities int64   `json:"filled_opportunities"`
	Revenue             float64 `json:"revenue"`
	GrossRevenue        float64 `json:"gross_revenue"`
}

type persistedDemandAnalytics struct {
	DemandID string `json:"demand_id"`
	persistedAnalyticsTotals
}

type persistedSupplyAnalytics struct {
	SupplyID int `json:"supply_id"`
	persistedAnalyticsTotals
}

type persistedBundleAnalytics struct {
	AppBundle string `json:"app_bundle"`
	persistedAnalyticsTotals
}

type persistedAnalyticsState struct {
	Totals  persistedAnalyticsTotals   `json:"totals"`
	Demand  []persistedDemandAnalytics `json:"demand"`
	Supply  []persistedSupplyAnalytics `json:"supply"`
	Bundles []persistedBundleAnalytics `json:"bundles"`
}

type analyticsOverviewResponse struct {
	monitor.Overview
	FilledOpportunities int64   `json:"filled_opportunities"`
	TotalRevenue        float64 `json:"total_revenue"`
	TotalGrossRevenue   float64 `json:"total_gross_revenue"`
	AdRequestFillRate   float64 `json:"ad_request_fill_rate"`
	OpportunityFillRate float64 `json:"opportunity_fill_rate"`
	NoBidRate           float64 `json:"no_bid_rate"`
	ECPM                float64 `json:"ecpm"`
	GrossCPM            float64 `json:"gross_cpm"`
}

type demandReportRow struct {
	ADomain                 string  `json:"adomain"`
	DemandID                string  `json:"demand_id"`
	DemandSourceID          int     `json:"demand_source_id,omitempty"`
	DemandType              string  `json:"demand_type,omitempty"`
	DemandName              string  `json:"demand_name,omitempty"`
	CreativeID              string  `json:"creative_id"`
	FilledOpportunities     int64   `json:"filled_opportunities"`
	Impressions             int64   `json:"impressions"`
	Revenue                 float64 `json:"revenue"`
	GrossRevenue            float64 `json:"gross_revenue"`
	ECPM                    float64 `json:"ecpm"`
	GrossCPM                float64 `json:"gross_cpm"`
	DemandRevenueTotal      float64 `json:"demand_revenue_total"`
	DemandGrossRevenueTotal float64 `json:"demand_gross_revenue_total"`
}

type supplyReportRow struct {
	SupplyID            int     `json:"supply_id"`
	SupplyName          string  `json:"supply_name,omitempty"`
	FilledOpportunities int64   `json:"filled_opportunities"`
	Revenue             float64 `json:"revenue"`
	GrossRevenue        float64 `json:"gross_revenue"`
	ECPM                float64 `json:"ecpm"`
	GrossCPM            float64 `json:"gross_cpm"`
}

type bundleReportRow struct {
	AppBundle           string  `json:"app_bundle"`
	FilledOpportunities int64   `json:"filled_opportunities"`
	Revenue             float64 `json:"revenue"`
	GrossRevenue        float64 `json:"gross_revenue"`
	ECPM                float64 `json:"ecpm"`
	GrossCPM            float64 `json:"gross_cpm"`
}

type analyticsAccumulator struct {
	FilledOpportunities int64
	Revenue             float64
	GrossRevenue        float64
}

func (a *analyticsAccumulator) addDecision(d AdDecision) {
	a.FilledOpportunities++
	a.Revenue += decisionNetRevenue(d)
	a.GrossRevenue += decisionGrossRevenue(d)
}

func analyticsAccumulatorFromPersistedTotals(t persistedAnalyticsTotals) analyticsAccumulator {
	return analyticsAccumulator{
		FilledOpportunities: t.FilledOpportunities,
		Revenue:             t.Revenue,
		GrossRevenue:        t.GrossRevenue,
	}
}

func persistedTotalsFromAccumulator(a analyticsAccumulator) persistedAnalyticsTotals {
	return persistedAnalyticsTotals{
		FilledOpportunities: a.FilledOpportunities,
		Revenue:             a.Revenue,
		GrossRevenue:        a.GrossRevenue,
	}
}

func (s *store) ensureAnalyticsTotalsLocked() {
	if s.analyticsDemandTotals == nil {
		s.analyticsDemandTotals = make(map[string]analyticsAccumulator)
	}
	if s.analyticsSupplyTotals == nil {
		s.analyticsSupplyTotals = make(map[int]analyticsAccumulator)
	}
	if s.analyticsBundleTotals == nil {
		s.analyticsBundleTotals = make(map[string]analyticsAccumulator)
	}
}

func (s *store) recordPersistedAnalyticsLocked(decision AdDecision) {
	s.ensureAnalyticsTotalsLocked()

	s.analyticsTotals.addDecision(decision)

	demandID := canonicalDemandID(decision)
	demandTotals := s.analyticsDemandTotals[demandID]
	demandTotals.addDecision(decision)
	s.analyticsDemandTotals[demandID] = demandTotals

	supplyTotals := s.analyticsSupplyTotals[decision.SupplyID]
	supplyTotals.addDecision(decision)
	s.analyticsSupplyTotals[decision.SupplyID] = supplyTotals

	bundleID := canonicalDecisionBundle(decision)
	if bundleID == "" {
		bundleID = "unknown"
	}
	bundleTotals := s.analyticsBundleTotals[bundleID]
	bundleTotals.addDecision(decision)
	s.analyticsBundleTotals[bundleID] = bundleTotals
}

func (s *store) loadPersistedAnalyticsLocked(snapshot persistedAnalyticsState) {
	s.analyticsTotals = analyticsAccumulatorFromPersistedTotals(snapshot.Totals)
	s.analyticsDemandTotals = make(map[string]analyticsAccumulator, len(snapshot.Demand))
	s.analyticsSupplyTotals = make(map[int]analyticsAccumulator, len(snapshot.Supply))
	s.analyticsBundleTotals = make(map[string]analyticsAccumulator, len(snapshot.Bundles))

	for _, demand := range snapshot.Demand {
		demandID := strings.TrimSpace(demand.DemandID)
		if demandID == "" {
			continue
		}
		s.analyticsDemandTotals[demandID] = analyticsAccumulatorFromPersistedTotals(demand.persistedAnalyticsTotals)
	}
	for _, supply := range snapshot.Supply {
		if supply.SupplyID < 0 {
			continue
		}
		s.analyticsSupplyTotals[supply.SupplyID] = analyticsAccumulatorFromPersistedTotals(supply.persistedAnalyticsTotals)
	}
	for _, bundle := range snapshot.Bundles {
		bundleID := strings.TrimSpace(bundle.AppBundle)
		if bundleID == "" {
			continue
		}
		s.analyticsBundleTotals[bundleID] = analyticsAccumulatorFromPersistedTotals(bundle.persistedAnalyticsTotals)
	}
}

func (s *store) snapshotPersistedAnalyticsLocked() persistedAnalyticsState {
	s.ensureAnalyticsTotalsLocked()

	out := persistedAnalyticsState{
		Totals:  persistedTotalsFromAccumulator(s.analyticsTotals),
		Demand:  make([]persistedDemandAnalytics, 0, len(s.analyticsDemandTotals)),
		Supply:  make([]persistedSupplyAnalytics, 0, len(s.analyticsSupplyTotals)),
		Bundles: make([]persistedBundleAnalytics, 0, len(s.analyticsBundleTotals)),
	}

	for demandID, totals := range s.analyticsDemandTotals {
		if strings.TrimSpace(demandID) == "" {
			continue
		}
		out.Demand = append(out.Demand, persistedDemandAnalytics{DemandID: demandID, persistedAnalyticsTotals: persistedTotalsFromAccumulator(totals)})
	}
	sort.Slice(out.Demand, func(i, j int) bool { return out.Demand[i].DemandID < out.Demand[j].DemandID })

	for supplyID, totals := range s.analyticsSupplyTotals {
		out.Supply = append(out.Supply, persistedSupplyAnalytics{SupplyID: supplyID, persistedAnalyticsTotals: persistedTotalsFromAccumulator(totals)})
	}
	sort.Slice(out.Supply, func(i, j int) bool { return out.Supply[i].SupplyID < out.Supply[j].SupplyID })

	for bundleID, totals := range s.analyticsBundleTotals {
		if strings.TrimSpace(bundleID) == "" {
			continue
		}
		out.Bundles = append(out.Bundles, persistedBundleAnalytics{AppBundle: bundleID, persistedAnalyticsTotals: persistedTotalsFromAccumulator(totals)})
	}
	sort.Slice(out.Bundles, func(i, j int) bool { return out.Bundles[i].AppBundle < out.Bundles[j].AppBundle })

	return out
}

func (s *store) snapshotAnalyticsState() analyticsState {
	state := analyticsState{
		decisions:    make([]AdDecision, 0),
		supplyNames:  make(map[int]string),
		demandNames:  make(map[string]string),
		demandTotals: make(map[string]analyticsAccumulator),
		supplyTotals: make(map[int]analyticsAccumulator),
		bundleTotals: make(map[string]analyticsAccumulator),
	}
	if s == nil {
		return state
	}

	s.decisionMu.RLock()
	state.decisions = append(state.decisions, s.adDecisions...)
	s.decisionMu.RUnlock()

	s.mu.RLock()
	for id, tag := range s.supplyTags {
		if tag == nil {
			continue
		}
		state.supplyNames[id] = strings.TrimSpace(tag.Name)
	}
	for id, endpoint := range s.demandEndpoints {
		if endpoint == nil {
			continue
		}
		state.demandNames[fmt.Sprintf("demand-ep-%d", id)] = strings.TrimSpace(endpoint.Name)
	}
	for id, tag := range s.demandVastTags {
		if tag == nil {
			continue
		}
		state.demandNames[fmt.Sprintf("demand-vast-%d", id)] = strings.TrimSpace(tag.Name)
	}
	state.allTimeTotals = s.analyticsTotals
	for demandID, totals := range s.analyticsDemandTotals {
		state.demandTotals[demandID] = totals
	}
	for supplyID, totals := range s.analyticsSupplyTotals {
		state.supplyTotals[supplyID] = totals
	}
	for bundleID, totals := range s.analyticsBundleTotals {
		state.bundleTotals[bundleID] = totals
	}
	s.mu.RUnlock()

	return state
}

func buildAnalyticsOverview(base monitor.Overview) analyticsOverviewResponse {
	filled := base.BidWins
	return analyticsOverviewResponse{
		Overview:            base,
		FilledOpportunities: filled,
		TotalRevenue:        base.TotalSpend,
		TotalGrossRevenue:   base.TotalGrossSpend,
		AdRequestFillRate:   analyticsPercent(filled, base.AdRequests),
		OpportunityFillRate: analyticsPercent(base.Impressions, base.AdOpps),
		NoBidRate:           analyticsPercent(base.NoBids, base.AdOpps),
		ECPM:                analyticsCPM(base.TotalSpend, filled),
		GrossCPM:            analyticsCPM(base.TotalGrossSpend, filled),
	}
}

func buildDemandReport(state analyticsState) []demandReportRow {
	totalsByDemand := make(map[string]analyticsAccumulator)
	usePersistedTotals := len(state.demandTotals) > 0
	if usePersistedTotals {
		for demandID, totals := range state.demandTotals {
			totalsByDemand[demandID] = totals
		}
	}
	rowsByKey := make(map[string]*demandReportRow)

	for _, decision := range state.decisions {
		demandID := canonicalDemandID(decision)
		if !usePersistedTotals {
			total := totalsByDemand[demandID]
			total.addDecision(decision)
			totalsByDemand[demandID] = total
		}

		key := demandID + "|" + strings.TrimSpace(decision.ADomain) + "|" + strings.TrimSpace(decision.CreativeID)
		row := rowsByKey[key]
		if row == nil {
			demandName, demandSourceID, demandType := state.demandMetadata(demandID)
			row = &demandReportRow{
				ADomain:        strings.TrimSpace(decision.ADomain),
				DemandID:       demandID,
				DemandSourceID: demandSourceID,
				DemandType:     demandType,
				DemandName:     demandName,
				CreativeID:     strings.TrimSpace(decision.CreativeID),
			}
			rowsByKey[key] = row
		}
		row.FilledOpportunities++
		row.Impressions = row.FilledOpportunities
		row.Revenue += decisionNetRevenue(decision)
		row.GrossRevenue += decisionGrossRevenue(decision)
		row.ECPM = analyticsCPM(row.Revenue, row.FilledOpportunities)
		row.GrossCPM = analyticsCPM(row.GrossRevenue, row.FilledOpportunities)
	}

	rows := make([]demandReportRow, 0, len(rowsByKey))
	for _, row := range rowsByKey {
		total := totalsByDemand[row.DemandID]
		row.DemandRevenueTotal = total.Revenue
		row.DemandGrossRevenueTotal = total.GrossRevenue
		rows = append(rows, *row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].DemandRevenueTotal == rows[j].DemandRevenueTotal {
			if rows[i].Revenue == rows[j].Revenue {
				if rows[i].DemandID == rows[j].DemandID {
					return rows[i].CreativeID < rows[j].CreativeID
				}
				return rows[i].DemandID < rows[j].DemandID
			}
			return rows[i].Revenue > rows[j].Revenue
		}
		return rows[i].DemandRevenueTotal > rows[j].DemandRevenueTotal
	})

	return rows
}

func buildSupplyReport(state analyticsState) []supplyReportRow {
	if len(state.supplyTotals) > 0 {
		rows := make([]supplyReportRow, 0, len(state.supplyTotals))
		for supplyID, totals := range state.supplyTotals {
			rows = append(rows, supplyReportRow{
				SupplyID:            supplyID,
				SupplyName:          state.supplyName(supplyID, ""),
				FilledOpportunities: totals.FilledOpportunities,
				Revenue:             totals.Revenue,
				GrossRevenue:        totals.GrossRevenue,
				ECPM:                analyticsCPM(totals.Revenue, totals.FilledOpportunities),
				GrossCPM:            analyticsCPM(totals.GrossRevenue, totals.FilledOpportunities),
			})
		}

		sort.Slice(rows, func(i, j int) bool {
			if rows[i].Revenue == rows[j].Revenue {
				return rows[i].SupplyID < rows[j].SupplyID
			}
			return rows[i].Revenue > rows[j].Revenue
		})

		return rows
	}

	rowsBySupply := make(map[int]*supplyReportRow)

	for _, decision := range state.decisions {
		supplyID := decision.SupplyID
		row := rowsBySupply[supplyID]
		if row == nil {
			row = &supplyReportRow{
				SupplyID:   supplyID,
				SupplyName: state.supplyName(supplyID, decision.Source),
			}
			rowsBySupply[supplyID] = row
		}
		row.FilledOpportunities++
		row.Revenue += decisionNetRevenue(decision)
		row.GrossRevenue += decisionGrossRevenue(decision)
		row.ECPM = analyticsCPM(row.Revenue, row.FilledOpportunities)
		row.GrossCPM = analyticsCPM(row.GrossRevenue, row.FilledOpportunities)
	}

	rows := make([]supplyReportRow, 0, len(rowsBySupply))
	for _, row := range rowsBySupply {
		rows = append(rows, *row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Revenue == rows[j].Revenue {
			return rows[i].SupplyID < rows[j].SupplyID
		}
		return rows[i].Revenue > rows[j].Revenue
	})

	return rows
}

func buildBundleReport(state analyticsState) []bundleReportRow {
	if len(state.bundleTotals) > 0 {
		rows := make([]bundleReportRow, 0, len(state.bundleTotals))
		for bundleID, totals := range state.bundleTotals {
			rows = append(rows, bundleReportRow{
				AppBundle:           bundleID,
				FilledOpportunities: totals.FilledOpportunities,
				Revenue:             totals.Revenue,
				GrossRevenue:        totals.GrossRevenue,
				ECPM:                analyticsCPM(totals.Revenue, totals.FilledOpportunities),
				GrossCPM:            analyticsCPM(totals.GrossRevenue, totals.FilledOpportunities),
			})
		}

		sort.Slice(rows, func(i, j int) bool {
			if rows[i].Revenue == rows[j].Revenue {
				return rows[i].AppBundle < rows[j].AppBundle
			}
			return rows[i].Revenue > rows[j].Revenue
		})

		return rows
	}

	rowsByBundle := make(map[string]*bundleReportRow)

	for _, decision := range state.decisions {
		bundle := canonicalDecisionBundle(decision)
		if bundle == "" {
			bundle = "unknown"
		}
		row := rowsByBundle[bundle]
		if row == nil {
			row = &bundleReportRow{AppBundle: bundle}
			rowsByBundle[bundle] = row
		}
		row.FilledOpportunities++
		row.Revenue += decisionNetRevenue(decision)
		row.GrossRevenue += decisionGrossRevenue(decision)
		row.ECPM = analyticsCPM(row.Revenue, row.FilledOpportunities)
		row.GrossCPM = analyticsCPM(row.GrossRevenue, row.FilledOpportunities)
	}

	rows := make([]bundleReportRow, 0, len(rowsByBundle))
	for _, row := range rowsByBundle {
		rows = append(rows, *row)
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Revenue == rows[j].Revenue {
			return rows[i].AppBundle < rows[j].AppBundle
		}
		return rows[i].Revenue > rows[j].Revenue
	})

	return rows
}

func analyticsPercent(numerator, denominator int64) float64 {
	if denominator <= 0 {
		return 0
	}
	return (float64(numerator) / float64(denominator)) * 100
}

func analyticsCPM(revenue float64, filledOpportunities int64) float64 {
	if filledOpportunities <= 0 {
		return 0
	}
	return (revenue / float64(filledOpportunities)) * 1000
}

func decisionNetRevenue(decision AdDecision) float64 {
	if decision.NetPrice <= 0 {
		return 0
	}
	return decision.NetPrice / 1000.0
}

func decisionGrossRevenue(decision AdDecision) float64 {
	grossPrice := decision.GrossPrice
	if grossPrice <= 0 {
		grossPrice = decision.BidPrice
	}
	if grossPrice <= 0 {
		return 0
	}
	return grossPrice / 1000.0
}

func canonicalDecisionBundle(decision AdDecision) string {
	if bundle := openrtb.CanonicalBundleValue(decision.AppBundle); bundle != "" {
		return bundle
	}
	return openrtb.CleanBundleValue(decision.RawBundle, "", "")
}

func canonicalDemandID(decision AdDecision) string {
	demandID := strings.TrimSpace(decision.DemandEp)
	if demandID == "" {
		demandID = strings.TrimSpace(decision.Seat)
	}
	if demandID == "" {
		return "unknown"
	}
	return demandID
}

func parseDemandSource(adapterID string) (int, string, bool) {
	switch {
	case strings.HasPrefix(adapterID, "demand-ep-"):
		id, err := strconv.Atoi(strings.TrimPrefix(adapterID, "demand-ep-"))
		return id, "ortb", err == nil && id > 0
	case strings.HasPrefix(adapterID, "demand-vast-"):
		id, err := strconv.Atoi(strings.TrimPrefix(adapterID, "demand-vast-"))
		return id, "vast", err == nil && id > 0
	default:
		return 0, "", false
	}
}

func (state analyticsState) demandMetadata(demandID string) (string, int, string) {
	name := strings.TrimSpace(state.demandNames[demandID])
	sourceID, demandType, ok := parseDemandSource(demandID)
	if name == "" {
		switch {
		case ok && sourceID > 0:
			name = fmt.Sprintf("#%d", sourceID)
		case strings.TrimSpace(demandID) != "":
			name = demandID
		default:
			name = "unknown"
		}
	}
	return name, sourceID, demandType
}

func (state analyticsState) supplyName(supplyID int, fallback string) string {
	if name := strings.TrimSpace(state.supplyNames[supplyID]); name != "" {
		return name
	}
	if trimmedFallback := strings.TrimSpace(fallback); trimmedFallback != "" {
		return trimmedFallback
	}
	if supplyID > 0 {
		return fmt.Sprintf("#%d", supplyID)
	}
	return "unknown"
}

type demandTotalsReportRow struct {
	DemandID            string  `json:"demand_id"`
	DemandSourceID      int     `json:"demand_source_id,omitempty"`
	DemandType          string  `json:"demand_type,omitempty"`
	DemandName          string  `json:"demand_name,omitempty"`
	FilledOpportunities int64   `json:"filled_opportunities"`
	Revenue             float64 `json:"revenue"`
	GrossRevenue        float64 `json:"gross_revenue"`
	ECPM                float64 `json:"ecpm"`
	GrossCPM            float64 `json:"gross_cpm"`
}

func buildDemandTotalsReport(state analyticsState) []demandTotalsReportRow {
	if len(state.demandTotals) == 0 {
		totals := make(map[string]analyticsAccumulator)
		for _, decision := range state.decisions {
			demandID := canonicalDemandID(decision)
			total := totals[demandID]
			total.addDecision(decision)
			totals[demandID] = total
		}
		state.demandTotals = totals
	}

	rows := make([]demandTotalsReportRow, 0, len(state.demandTotals))
	for demandID, totals := range state.demandTotals {
		demandName, demandSourceID, demandType := state.demandMetadata(demandID)
		rows = append(rows, demandTotalsReportRow{
			DemandID:            demandID,
			DemandSourceID:      demandSourceID,
			DemandType:          demandType,
			DemandName:          demandName,
			FilledOpportunities: totals.FilledOpportunities,
			Revenue:             totals.Revenue,
			GrossRevenue:        totals.GrossRevenue,
			ECPM:                analyticsCPM(totals.Revenue, totals.FilledOpportunities),
			GrossCPM:            analyticsCPM(totals.GrossRevenue, totals.FilledOpportunities),
		})
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Revenue == rows[j].Revenue {
			return rows[i].DemandID < rows[j].DemandID
		}
		return rows[i].Revenue > rows[j].Revenue
	})

	return rows
}
