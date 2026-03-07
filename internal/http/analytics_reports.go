package http

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

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
	Totals        persistedAnalyticsTotals     `json:"totals"`
	Demand        []persistedDemandAnalytics   `json:"demand"`
	Supply        []persistedSupplyAnalytics   `json:"supply"`
	Bundles       []persistedBundleAnalytics   `json:"bundles"`
	HourlyMetrics []monitor.HourlyMetricBucket `json:"hourly_metrics,omitempty"`
	ExportMetrics []metricsExportBucket        `json:"export_metrics,omitempty"`
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

type metricsExportQuery struct {
	Preset           string    `json:"preset"`
	GroupBy          string    `json:"group_by"`
	Timezone         string    `json:"timezone"`
	StartDate        string    `json:"start_date"`
	EndDate          string    `json:"end_date"`
	StartHour        int       `json:"start_hour"`
	EndHour          int       `json:"end_hour"`
	StartTime        time.Time `json:"start_time"`
	EndTimeExclusive time.Time `json:"end_time_exclusive"`
}

type metricsExportBucket struct {
	Hour                time.Time `json:"hour"`
	SourceID            int       `json:"source_id,omitempty"`
	DemandEndpointID    int       `json:"demand_endpoint_id,omitempty"`
	LegacyCampaignID    int       `json:"campaign_id,omitempty"`
	CountryCode         string    `json:"country_code,omitempty"`
	BundleID            string    `json:"bundle_id,omitempty"`
	AdRequests          int64     `json:"ad_requests"`
	AdOpportunities     int64     `json:"ad_opportunities"`
	FilledOpportunities int64     `json:"filled_opportunities"`
	Impressions         int64     `json:"impressions"`
	SourceIDRevenue     float64   `json:"source_id_revenue,omitempty"`
	LegacySourceMarginRevenue float64 `json:"source_margin_revenue,omitempty"`
	LegacyChannelRevenue float64  `json:"channel_revenue,omitempty"`
	TotalRevenue        float64   `json:"total_revenue"`
}

type metricsExportDeliveryContext struct {
	RecordedAt        time.Time
	SourceID          int
	DemandEndpointID  int
	CountryCode       string
	BundleID          string
}

type metricsExportAccumulator struct {
	AdRequests          int64
	AdOpportunities     int64
	FilledOpportunities int64
	Impressions         int64
	SourceIDRevenue     float64
	TotalRevenue        float64
}

type metricsExportRow struct {
	Date                string  `json:"date,omitempty"`
	Hour                string  `json:"hour,omitempty"`
	SourceID            int     `json:"source_id,omitempty"`
	DemandEndpointID    int     `json:"demand_endpoint_id,omitempty"`
	CountryCode         string  `json:"country_code,omitempty"`
	Country             string  `json:"country,omitempty"`
	BundleID            string  `json:"bundle_id,omitempty"`
	AdRequests          int64   `json:"ad_requests"`
	AdOpportunities     int64   `json:"ad_opportunities"`
	Impressions         int64   `json:"impressions"`
	SourceIDRevenue     float64 `json:"source_id_revenue"`
	SourceIDECPM        float64 `json:"source_id_ecpm"`
	TotalRevenue        float64 `json:"total_revenue"`
	ECPM                float64 `json:"ecpm"`
	AdRequestFillRate   float64 `json:"ad_request_fill_rate"`
	OpportunityFillRate float64 `json:"opportunity_fill_rate"`
}

type metricsExportResponse struct {
	Preset           string             `json:"preset"`
	GroupBy          string             `json:"group_by"`
	Timezone         string             `json:"timezone"`
	StartDate        string             `json:"start_date"`
	EndDate          string             `json:"end_date"`
	StartHour        int                `json:"start_hour"`
	EndHour          int                `json:"end_hour"`
	StartTime        time.Time          `json:"start_time"`
	EndTimeExclusive time.Time          `json:"end_time_exclusive"`
	Rows             []metricsExportRow `json:"rows"`
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
	if s.metrics != nil {
		s.metrics.LoadHourlyMetrics(snapshot.HourlyMetrics)
	}
	s.loadMetricsExportBucketsLocked(snapshot.ExportMetrics)
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
	if s.metrics != nil {
		out.HourlyMetrics = s.metrics.SnapshotHourlyMetrics()
	}
	out.ExportMetrics = s.snapshotMetricsExportBucketsLocked()

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

func (s *store) ensureMetricsExportStateLocked() {
	if s.metricsExportBuckets == nil {
		s.metricsExportBuckets = make(map[string]metricsExportBucket)
	}
	if s.metricsExportDelivery == nil {
		s.metricsExportDelivery = make(map[string]metricsExportDeliveryContext)
	}
}

func (s *store) loadMetricsExportBucketsLocked(buckets []metricsExportBucket) {
	s.ensureMetricsExportStateLocked()
	s.metricsExportBuckets = make(map[string]metricsExportBucket, len(buckets))
	s.metricsExportDelivery = make(map[string]metricsExportDeliveryContext)
	for _, bucket := range buckets {
		hour := bucket.Hour.UTC().Truncate(time.Hour)
		if hour.IsZero() {
			continue
		}
		bucket.Hour = hour
		if bucket.DemandEndpointID <= 0 && bucket.LegacyCampaignID > 0 {
			bucket.DemandEndpointID = bucket.LegacyCampaignID
		}
		if bucket.SourceIDRevenue <= 0 && bucket.LegacyChannelRevenue > 0 {
			bucket.SourceIDRevenue = bucket.LegacyChannelRevenue
		}
		if bucket.SourceIDRevenue <= 0 && bucket.LegacySourceMarginRevenue > 0 {
			bucket.SourceIDRevenue = metricsSupplyRevenue(bucket.TotalRevenue, bucket.LegacySourceMarginRevenue)
		}
		bucket.LegacyCampaignID = 0
		bucket.LegacySourceMarginRevenue = 0
		bucket.LegacyChannelRevenue = 0
		bucket.CountryCode = normalizeMetricsCountryCode(bucket.CountryCode)
		bucket.BundleID = normalizeMetricsBundleID(bucket.BundleID)
		s.metricsExportBuckets[metricsExportBucketKey(bucket)] = bucket
	}
}

func (s *store) snapshotMetricsExportBucketsLocked() []metricsExportBucket {
	if len(s.metricsExportBuckets) == 0 {
		return nil
	}
	out := make([]metricsExportBucket, 0, len(s.metricsExportBuckets))
	for _, bucket := range s.metricsExportBuckets {
		out = append(out, bucket)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Hour.Equal(out[j].Hour) {
			if out[i].SourceID == out[j].SourceID {
				if out[i].DemandEndpointID == out[j].DemandEndpointID {
					if out[i].CountryCode == out[j].CountryCode {
						return out[i].BundleID < out[j].BundleID
					}
					return out[i].CountryCode < out[j].CountryCode
				}
				return out[i].DemandEndpointID < out[j].DemandEndpointID
			}
			return out[i].SourceID < out[j].SourceID
		}
		return out[i].Hour.Before(out[j].Hour)
	})
	return out
}

func (s *store) snapshotMetricsExportBuckets() []metricsExportBucket {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.snapshotMetricsExportBucketsLocked()
}

func (s *store) addMetricsExportBucketLocked(bucket metricsExportBucket) {
	bucket.Hour = bucket.Hour.UTC().Truncate(time.Hour)
	bucket.CountryCode = normalizeMetricsCountryCode(bucket.CountryCode)
	bucket.BundleID = normalizeMetricsBundleID(bucket.BundleID)
	key := metricsExportBucketKey(bucket)
	existing := s.metricsExportBuckets[key]
	if existing.Hour.IsZero() {
		existing.Hour = bucket.Hour
		existing.SourceID = bucket.SourceID
		existing.DemandEndpointID = bucket.DemandEndpointID
		existing.CountryCode = bucket.CountryCode
		existing.BundleID = bucket.BundleID
	}
	existing.AdRequests += bucket.AdRequests
	existing.AdOpportunities += bucket.AdOpportunities
	existing.FilledOpportunities += bucket.FilledOpportunities
	existing.Impressions += bucket.Impressions
	existing.SourceIDRevenue += bucket.SourceIDRevenue
	existing.TotalRevenue += bucket.TotalRevenue
	s.metricsExportBuckets[key] = existing
}

func (s *store) recordMetricsExportRequestOutcome(req *openrtb.BidRequest, sourceID, demandEndpointID int, bundleID string, filledOpportunities int64, sourceRevenue, totalRevenue float64) {
	if s == nil {
		return
	}

	recordedAt := time.Now().UTC()
	requestID := ""
	countryCode := ""
	if req != nil {
		requestID = strings.TrimSpace(req.ID)
		if req.Device != nil && req.Device.Geo != nil {
			countryCode = req.Device.Geo.Country
		}
	}

	bucket := metricsExportBucket{
		Hour:                recordedAt.Truncate(time.Hour),
		SourceID:            sourceID,
		DemandEndpointID:    demandEndpointID,
		CountryCode:         countryCode,
		BundleID:            bundleID,
		AdRequests:          1,
		AdOpportunities:     1,
		FilledOpportunities: filledOpportunities,
		SourceIDRevenue:     sourceRevenue,
		TotalRevenue:        totalRevenue,
	}

	s.mu.Lock()
	s.ensureMetricsExportStateLocked()
	s.addMetricsExportBucketLocked(bucket)
	if requestID != "" && filledOpportunities > 0 {
		s.metricsExportDelivery[requestID] = metricsExportDeliveryContext{
			RecordedAt:       recordedAt,
			SourceID:         bucket.SourceID,
			DemandEndpointID: bucket.DemandEndpointID,
			CountryCode:      normalizeMetricsCountryCode(bucket.CountryCode),
			BundleID:         normalizeMetricsBundleID(bucket.BundleID),
		}
	}
	s.pruneMetricsExportDeliveryLocked(recordedAt)
	s.stateGeneration.Add(1)
	s.mu.Unlock()
	s.scheduleDeferredStatePersist()
}

func (s *store) recordMetricsExportImpression(requestID, countryCode, bundleID string) {
	if s == nil {
		return
	}

	recordedAt := time.Now().UTC()
	requestID = strings.TrimSpace(requestID)
	context := metricsExportDeliveryContext{
		RecordedAt:  recordedAt,
		CountryCode: normalizeMetricsCountryCode(countryCode),
		BundleID:    normalizeMetricsBundleID(bundleID),
	}

	s.mu.Lock()
	s.ensureMetricsExportStateLocked()
	if requestID != "" {
		if stored, ok := s.metricsExportDelivery[requestID]; ok {
			context = stored
			context.RecordedAt = recordedAt
			s.metricsExportDelivery[requestID] = context
		}
	}
	s.addMetricsExportBucketLocked(metricsExportBucket{
		Hour:             recordedAt.Truncate(time.Hour),
		SourceID:         context.SourceID,
		DemandEndpointID: context.DemandEndpointID,
		CountryCode:      context.CountryCode,
		BundleID:         context.BundleID,
		Impressions:      1,
	})
	s.pruneMetricsExportDeliveryLocked(recordedAt)
	s.stateGeneration.Add(1)
	s.mu.Unlock()
	s.scheduleDeferredStatePersist()
}

func (s *store) pruneMetricsExportDeliveryLocked(now time.Time) {
	if len(s.metricsExportDelivery) == 0 {
		return
	}
	cutoff := now.Add(-48 * time.Hour)
	for requestID, context := range s.metricsExportDelivery {
		if context.RecordedAt.Before(cutoff) {
			delete(s.metricsExportDelivery, requestID)
		}
	}
	if len(s.metricsExportDelivery) <= 10000 {
		return
	}
	toTrim := len(s.metricsExportDelivery) - 10000
	for requestID, context := range s.metricsExportDelivery {
		if context.RecordedAt.Before(now.Add(-6 * time.Hour)) {
			delete(s.metricsExportDelivery, requestID)
			toTrim--
			if toTrim <= 0 {
				return
			}
		}
	}
	for requestID := range s.metricsExportDelivery {
		delete(s.metricsExportDelivery, requestID)
		toTrim--
		if toTrim <= 0 {
			return
		}
	}
}

func metricsExportBucketKey(bucket metricsExportBucket) string {
	return fmt.Sprintf("%s|%d|%d|%s|%s",
		bucket.Hour.UTC().Truncate(time.Hour).Format(time.RFC3339),
		bucket.SourceID,
		bucket.DemandEndpointID,
		normalizeMetricsCountryCode(bucket.CountryCode),
		normalizeMetricsBundleID(bucket.BundleID),
	)
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

func metricsSupplyRevenue(grossRevenue, marginRevenue float64) float64 {
	supplyRevenue := grossRevenue - marginRevenue
	if supplyRevenue <= 0 {
		return 0
	}
	return supplyRevenue
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

func resolveMetricsExportQuery(preset, groupBy, startDate, endDate, startHourRaw, endHourRaw string, now time.Time) (metricsExportQuery, error) {
	preset = strings.ToLower(strings.TrimSpace(preset))
	if preset == "" {
		preset = "today"
	}

	groupBy = strings.ToLower(strings.TrimSpace(groupBy))
	if groupBy == "" {
		if preset == "custom" {
			groupBy = "date"
		} else {
			groupBy = "summary"
		}
	}
	if groupBy != "summary" && groupBy != "date" && groupBy != "hour" {
		return metricsExportQuery{}, fmt.Errorf("invalid group_by %q", groupBy)
	}

	utcNow := now.UTC()
	todayStart := time.Date(utcNow.Year(), utcNow.Month(), utcNow.Day(), 0, 0, 0, 0, time.UTC)

	query := metricsExportQuery{
		Preset:   preset,
		GroupBy:  groupBy,
		Timezone: "UTC",
	}

	switch preset {
	case "today":
		query.StartTime = todayStart
		query.EndTimeExclusive = todayStart.Add(24 * time.Hour)
	case "yesterday":
		query.StartTime = todayStart.Add(-24 * time.Hour)
		query.EndTimeExclusive = todayStart
	case "month":
		query.StartTime = time.Date(utcNow.Year(), utcNow.Month(), 1, 0, 0, 0, 0, time.UTC)
		query.EndTimeExclusive = todayStart.Add(24 * time.Hour)
	case "custom":
		startDay, err := parseMetricsExportDate(startDate)
		if err != nil {
			return metricsExportQuery{}, fmt.Errorf("invalid start_date: %w", err)
		}
		endDay, err := parseMetricsExportDate(endDate)
		if err != nil {
			return metricsExportQuery{}, fmt.Errorf("invalid end_date: %w", err)
		}
		startHour, err := parseMetricsExportHour(startHourRaw, 0)
		if err != nil {
			return metricsExportQuery{}, fmt.Errorf("invalid start_hour: %w", err)
		}
		endHour, err := parseMetricsExportHour(endHourRaw, 23)
		if err != nil {
			return metricsExportQuery{}, fmt.Errorf("invalid end_hour: %w", err)
		}
		query.StartTime = time.Date(startDay.Year(), startDay.Month(), startDay.Day(), startHour, 0, 0, 0, time.UTC)
		query.EndTimeExclusive = time.Date(endDay.Year(), endDay.Month(), endDay.Day(), endHour, 0, 0, 0, time.UTC).Add(time.Hour)
	default:
		return metricsExportQuery{}, fmt.Errorf("invalid preset %q", preset)
	}

	if !query.EndTimeExclusive.After(query.StartTime) {
		return metricsExportQuery{}, fmt.Errorf("range end must be after range start")
	}

	query.StartDate = query.StartTime.Format("2006-01-02")
	query.EndDate = query.EndTimeExclusive.Add(-time.Hour).Format("2006-01-02")
	query.StartHour = query.StartTime.Hour()
	query.EndHour = query.EndTimeExclusive.Add(-time.Hour).Hour()
	return query, nil
}

func buildMetricsExportResponse(buckets []metricsExportBucket, query metricsExportQuery) metricsExportResponse {
	return metricsExportResponse{
		Preset:           query.Preset,
		GroupBy:          query.GroupBy,
		Timezone:         query.Timezone,
		StartDate:        query.StartDate,
		EndDate:          query.EndDate,
		StartHour:        query.StartHour,
		EndHour:          query.EndHour,
		StartTime:        query.StartTime,
		EndTimeExclusive: query.EndTimeExclusive,
		Rows:             buildMetricsExportRows(buckets, query),
	}
}

func buildMetricsExportRows(buckets []metricsExportBucket, query metricsExportQuery) []metricsExportRow {
	if len(buckets) == 0 {
		return make([]metricsExportRow, 0)
	}

	type groupedRow struct {
		stamp            time.Time
		date             string
		hour             string
		sourceID         int
		demandEndpointID int
		countryCode      string
		bundleID         string
		acc              metricsExportAccumulator
	}

	rowsByKey := make(map[string]*groupedRow)
	for _, bucket := range buckets {
		hour := bucket.Hour.UTC().Truncate(time.Hour)
		if hour.IsZero() {
			continue
		}
		if hour.Before(query.StartTime) || !hour.Before(query.EndTimeExclusive) {
			continue
		}

		date := ""
		rowHour := ""
		stamp := query.StartTime
		timeKey := "summary"
		switch query.GroupBy {
		case "date":
			date = hour.Format("2006-01-02")
			timeKey = date
			stamp = time.Date(hour.Year(), hour.Month(), hour.Day(), 0, 0, 0, 0, time.UTC)
		case "hour":
			date = hour.Format("2006-01-02")
			rowHour = hour.Format("15:00")
			timeKey = hour.Format(time.RFC3339)
			stamp = hour
		}

		countryCode := normalizeMetricsCountryCode(bucket.CountryCode)
		bundleID := normalizeMetricsBundleID(bucket.BundleID)
		key := fmt.Sprintf("%s|%d|%d|%s|%s", timeKey, bucket.SourceID, bucket.DemandEndpointID, countryCode, bundleID)

		row := rowsByKey[key]
		if row == nil {
			row = &groupedRow{
				stamp:            stamp,
				date:             date,
				hour:             rowHour,
				sourceID:         bucket.SourceID,
				demandEndpointID: bucket.DemandEndpointID,
				countryCode:      countryCode,
				bundleID:         bundleID,
			}
			rowsByKey[key] = row
		}
		row.acc.addBucket(bucket)
	}

	ordered := make([]groupedRow, 0, len(rowsByKey))
	for _, row := range rowsByKey {
		ordered = append(ordered, *row)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].stamp.Equal(ordered[j].stamp) {
			if ordered[i].sourceID == ordered[j].sourceID {
				if ordered[i].demandEndpointID == ordered[j].demandEndpointID {
					if ordered[i].countryCode == ordered[j].countryCode {
						return ordered[i].bundleID < ordered[j].bundleID
					}
					return ordered[i].countryCode < ordered[j].countryCode
				}
				return ordered[i].demandEndpointID < ordered[j].demandEndpointID
			}
			return ordered[i].sourceID < ordered[j].sourceID
		}
		return ordered[i].stamp.Before(ordered[j].stamp)
	})

	rows := make([]metricsExportRow, 0, len(ordered))
	for _, row := range ordered {
		rows = append(rows, metricsExportRow{
			Date:                row.date,
			Hour:                row.hour,
			SourceID:            row.sourceID,
			DemandEndpointID:    row.demandEndpointID,
			CountryCode:         row.countryCode,
			Country:             metricsCountryName(row.countryCode),
			BundleID:            row.bundleID,
			AdRequests:          row.acc.AdRequests,
			AdOpportunities:     row.acc.AdOpportunities,
			Impressions:         row.acc.Impressions,
			SourceIDRevenue:     row.acc.SourceIDRevenue,
			SourceIDECPM:        analyticsCPM(row.acc.SourceIDRevenue, row.acc.FilledOpportunities),
			TotalRevenue:        row.acc.TotalRevenue,
			AdRequestFillRate:   analyticsPercent(row.acc.FilledOpportunities, row.acc.AdRequests),
			OpportunityFillRate: analyticsPercent(row.acc.Impressions, row.acc.AdOpportunities),
			ECPM:                analyticsCPM(row.acc.TotalRevenue, row.acc.FilledOpportunities),
		})
	}

	if rows == nil {
		return make([]metricsExportRow, 0)
	}
	return rows
}

func renderMetricsExportCSV(rows []metricsExportRow, groupBy string) (string, error) {
	buf := &bytes.Buffer{}
	writer := csv.NewWriter(buf)
	headers := make([]string, 0, 15)
	if groupBy == "date" || groupBy == "hour" {
		headers = append(headers, "Date")
	}
	if groupBy == "hour" {
		headers = append(headers, "Hour")
	}
	headers = append(headers,
		"Source ID",
		"Demand ORTB Endpoint ID",
		"Country Code",
		"Country",
		"Bundle ID",
		"Ad Requests",
		"Ad Opportunities",
		"Impressions",
		"Source ID Revenue",
		"Source ID eCPM",
		"Demand ORTB Endpoints Revenue",
		"eCPM",
		"Fill Rate (Ad Req)",
		"Fill Rate (Ad Ops)",
	)
	if err := writer.Write(headers); err != nil {
		return "", err
	}

	for _, row := range rows {
		record := make([]string, 0, len(headers))
		if groupBy == "date" || groupBy == "hour" {
			record = append(record, row.Date)
		}
		if groupBy == "hour" {
			record = append(record, row.Hour)
		}
		record = append(record,
			metricsExportIntString(row.SourceID),
			metricsExportIntString(row.DemandEndpointID),
			row.CountryCode,
			row.Country,
			row.BundleID,
			strconv.FormatInt(row.AdRequests, 10),
			strconv.FormatInt(row.AdOpportunities, 10),
			strconv.FormatInt(row.Impressions, 10),
			fmt.Sprintf("%.6f", row.SourceIDRevenue),
			fmt.Sprintf("%.2f", row.SourceIDECPM),
			fmt.Sprintf("%.6f", row.TotalRevenue),
			fmt.Sprintf("%.2f", row.ECPM),
			fmt.Sprintf("%.2f", row.AdRequestFillRate),
			fmt.Sprintf("%.2f", row.OpportunityFillRate),
		)
		if err := writer.Write(record); err != nil {
			return "", err
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func parseMetricsExportDate(raw string) (time.Time, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return time.Time{}, fmt.Errorf("value is required")
	}
	parsed, err := time.ParseInLocation("2006-01-02", value, time.UTC)
	if err != nil {
		return time.Time{}, err
	}
	return parsed.UTC(), nil
}

func parseMetricsExportHour(raw string, fallback int) (int, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fallback, nil
	}
	hour, err := strconv.Atoi(value)
	if err != nil {
		return 0, err
	}
	if hour < 0 || hour > 23 {
		return 0, fmt.Errorf("must be between 0 and 23")
	}
	return hour, nil
}

func normalizeMetricsCountryCode(raw string) string {
	code := strings.ToUpper(strings.TrimSpace(raw))
	if code == "" {
		return "unknown"
	}
	if len(code) == 2 {
		return openrtb.ToAlpha3(code)
	}
	return code
}

func normalizeMetricsBundleID(raw string) string {
	bundleID := strings.TrimSpace(raw)
	if bundleID == "" {
		return "unknown"
	}
	return bundleID
}

func metricsExportIntString(value int) string {
	if value <= 0 {
		return ""
	}
	return strconv.Itoa(value)
}

func metricsCountryName(code string) string {
	code = normalizeMetricsCountryCode(code)
	if code == "unknown" {
		return "Unknown"
	}
	if name := metricsCountryNames[code]; name != "" {
		return name
	}
	return code
}

var metricsCountryNames = map[string]string{
	"ARG": "Argentina",
	"ARE": "United Arab Emirates",
	"AUS": "Australia",
	"AUT": "Austria",
	"BEL": "Belgium",
	"BGD": "Bangladesh",
	"BGR": "Bulgaria",
	"BRA": "Brazil",
	"CAN": "Canada",
	"CHE": "Switzerland",
	"CHL": "Chile",
	"CHN": "China",
	"COL": "Colombia",
	"CRI": "Costa Rica",
	"CUB": "Cuba",
	"CZE": "Czech Republic",
	"DEU": "Germany",
	"DNK": "Denmark",
	"DOM": "Dominican Republic",
	"ECU": "Ecuador",
	"EGY": "Egypt",
	"ESP": "Spain",
	"EST": "Estonia",
	"FIN": "Finland",
	"FRA": "France",
	"GBR": "United Kingdom",
	"GRC": "Greece",
	"GTM": "Guatemala",
	"HKG": "Hong Kong",
	"HRV": "Croatia",
	"HUN": "Hungary",
	"IDN": "Indonesia",
	"IND": "India",
	"IRL": "Ireland",
	"ISR": "Israel",
	"ITA": "Italy",
	"JAM": "Jamaica",
	"JPN": "Japan",
	"KEN": "Kenya",
	"KOR": "South Korea",
	"LTU": "Lithuania",
	"LVA": "Latvia",
	"MEX": "Mexico",
	"MYS": "Malaysia",
	"NGA": "Nigeria",
	"NLD": "Netherlands",
	"NOR": "Norway",
	"NZL": "New Zealand",
	"PAK": "Pakistan",
	"PAN": "Panama",
	"PER": "Peru",
	"PHL": "Philippines",
	"POL": "Poland",
	"PRI": "Puerto Rico",
	"PRT": "Portugal",
	"ROU": "Romania",
	"SAU": "Saudi Arabia",
	"SGP": "Singapore",
	"SVK": "Slovakia",
	"SVN": "Slovenia",
	"SWE": "Sweden",
	"THA": "Thailand",
	"TUR": "Turkey",
	"TWN": "Taiwan",
	"UKR": "Ukraine",
	"USA": "United States",
	"VEN": "Venezuela",
	"VNM": "Vietnam",
	"ZAF": "South Africa",
}

func (a *metricsExportAccumulator) addBucket(bucket metricsExportBucket) {
	a.AdRequests += bucket.AdRequests
	a.AdOpportunities += bucket.AdOpportunities
	a.FilledOpportunities += bucket.FilledOpportunities
	a.Impressions += bucket.Impressions
	a.SourceIDRevenue += bucket.SourceIDRevenue
	a.TotalRevenue += bucket.TotalRevenue
}
