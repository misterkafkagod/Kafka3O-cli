package groups

import "math/big"

// TotalLag is a port of the UI's consumerGroupTotalLag: the exact sum of the partitions' lags; a partition
// without a lag makes the total unknown, never zero.
func TotalLag(group ConsumerGroup) Lag {
	if len(group.Partitions) == 0 {
		return Lag{Kind: "none"}
	}
	unknown := 0
	sum := new(big.Int)
	for _, partition := range group.Partitions {
		lag, ok := new(big.Int), partition.Lag != nil
		if ok {
			_, ok = lag.SetString(*partition.Lag, 10)
		}
		if !ok {
			unknown++
			continue
		}
		sum.Add(sum, lag)
	}
	if unknown > 0 {
		return Lag{Kind: "unknown", Partitions: unknown}
	}
	return Lag{Kind: "known", Lag: sum.String()}
}
