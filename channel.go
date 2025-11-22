package mycelia

// -------Channel Selection Strategies------------------------------------------

type SelectionStrat uint8

const (
	SelectionStratRandom     SelectionStrat = 0
	SelectionStratRoundrobin SelectionStrat = 1
	SelectionStratPubsub     SelectionStrat = 2
)

var selStratName = map[SelectionStrat]string{
	SelectionStratRandom:     "random",
	SelectionStratRoundrobin: "round-robin",
	SelectionStratPubsub:     "pub-sub",
}

func (ss SelectionStrat) String() string {
	return selStratName[ss]
}
