package auction

// DynamicFloor computes a floor price from the recent average.
// Uses 70% of average win price as the dynamic floor.
func DynamicFloor(avg float64) float64 {
	if avg <= 0 {
		return 0
	}
	return avg * 0.7
}
