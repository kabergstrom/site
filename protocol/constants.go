package protocol

// SourceID enum for where a post comes from
type SourceID uint8

const (
	// Site ourselves
	Site SourceID = 1
	// HackerNews hacker news
	HackerNews SourceID = 2
)

// ObjectType type of a user or post
type ObjectType uint8

const (
	// LinkPost link post with title
	LinkPost ObjectType = 1
	// Comment comment on another post
	Comment ObjectType = 2
	// TextPost post with text and title
	TextPost ObjectType = 3
	// Job listing of a job opening
	Job ObjectType = 4
	// Poll poll with PollOpts
	Poll ObjectType = 5
	// PollOpt option for a poll
	PollOpt ObjectType = 6
	// User user object
	User ObjectType = 7
)
