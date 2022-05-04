package shardmaster

import "fmt"

// 10 / 4 --> [3, 3, 2, 2]
func integerDivide(divident, divisor int) []int {
	baseCnt := divident / divisor      // 2
	left := divident - baseCnt*divisor // 2
	ans := make([]int, divisor)
	for i := 0; i < divisor; i++ {
		if i < left {
			ans[i] = baseCnt + 1
		} else {
			ans[i] = baseCnt
		}
	}

	return ans
}

func sortInput(oldArr, newArr []int) ([]int, []int) {
	var sortedOld, sortedNew, crossSet, newSet []int
	crossSetMap, oldItemSet := map[int]bool{}, map[int]bool{}
	for _, item := range oldArr {
		oldItemSet[item] = true
	}

	for _, item := range newArr {
		if _, ok := oldItemSet[item]; ok {
			crossSet = append(crossSet, item)
			crossSetMap[item] = true
			sortedNew = append(sortedNew, item)
		} else {
			newSet = append(newSet, item)
		}
	}

	sortedNew = append(sortedNew, newSet...)
	sortedOld = append(sortedOld, crossSet...)
	for _, item := range oldArr {
		if _, ok := crossSetMap[item]; !ok {
			sortedOld = append(sortedOld, item)
		}
	}

	return sortedOld, sortedNew
}


func rebalance(oldShards [NShards]int, oldGrps, newGrps []int) ([NShards]int, int) {
	oldGrps, newGrps = sortInput(oldGrps, newGrps)
	movedShards := 0
	// It's wired but test cases will reduce the num of groups to zero
	if len(newGrps) == 0 {
		var result [NShards]int
		for i:=0;i<NShards;i++ {
			if oldShards[i] != 0 {
				movedShards++
			}
			result[i] = 0
		}
		return result, movedShards
	}

	moveInToken := []int{}
	newShardsIdx := make([]int, len(oldShards))
	var newShards [NShards]int

	bucketNum := len(oldGrps) // 5
	if len(newGrps) > len(oldGrps) {
		bucketNum = len(newGrps)
	}

	oldGID2Idx := map[int]int{}
	for idx, GID := range oldGrps {
		oldGID2Idx[GID] = idx
	}
	newGID2Idx := map[int]int{}
	for idx, GID := range newGrps {
		newGID2Idx[GID] = idx
	}

	oldShardsNumInBuckets := make(map[int]int)
	for _, GID := range oldShards { // [0120120120]
		oldShardsNumInBuckets[oldGID2Idx[GID]]++
	} // [4,3,3]

	newShardsNumInBuckets := integerDivide(NShards, len(newGrps)) // [2,2,2,2,2]

	// Extend newShards
	for i := len(newGrps); i < len(oldGrps); i++ {
		newShardsNumInBuckets = append(newShardsNumInBuckets, 0)
	}

	delta := make([]int, bucketNum) // [-2, -1, -1, +2, +2]
	for idx := 0; idx < bucketNum; idx++ {
		delta[idx] = newShardsNumInBuckets[idx] - oldShardsNumInBuckets[idx]
		if delta[idx] > 0 {
			movedShards += delta[idx]
			for j := 0; j < delta[idx]; j++ {
				moveInToken = append(moveInToken, idx)
			} // [3 3 4 4]
		}
	}

	for shardID := 0; shardID < NShards; shardID++ {
		bucketIdx := oldGID2Idx[oldShards[shardID]]
		if delta[bucketIdx] < 0 { // Need to move to a new group
			newIdx := moveInToken[0]
			newShardsIdx[shardID] = newIdx
			moveInToken = moveInToken[1:]
			delta[newIdx]--
			delta[bucketIdx]++
		} else {
			newShardsIdx[shardID] = oldGID2Idx[oldShards[shardID]]
		}
	}

	for shardID := 0; shardID < NShards; shardID++ {
		newShards[shardID] = newGrps[newShardsIdx[shardID]]
	}

	return newShards, movedShards
}


func SortTestCases() {
	fmt.Println(sortInput([]int{3,4,6,7,5}, []int{7,4,2,6}))
	fmt.Println(sortInput([]int{1,2,3,4}, []int{1,2,4,3}))
	fmt.Println(sortInput([]int{1,2,3,4}, []int{2,4,3}))
}

func RebalanceTestCases() {
	fmt.Println(rebalance(
		[NShards]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		[]int{0},
		[]int{10, 11, 12, 33, 44}))
	fmt.Println(rebalance(
		[NShards]int{10, 11, 12, 10, 11, 12, 10, 11, 12, 10},
		[]int{10, 11, 12},
		[]int{10, 11, 12, 33, 44}))
	fmt.Println(rebalance(
		[NShards]int{10, 11, 12, 10, 11, 12, 33, 44, 44, 33},
		[]int{10, 11, 12, 33, 44},
		[]int{10, 11, 12}))
	fmt.Println(rebalance(
		[NShards]int{10, 11, 12, 10, 11, 12, 33, 44, 44, 33},
		[]int{10, 11, 12, 33, 44},
		[]int{10, 11, 12, 44, 33}))
}
