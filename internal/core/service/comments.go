package service

import (
	"sort"
	"time"
)

// CommentPosition is a domain keyset position. The ID is the one-based
// ordinal in the stored append-only comment array. Existing stored documents
// need no migration, and comments with equal timestamps have a stable order.
type CommentPosition struct {
	Time time.Time `json:"time"`
	ID   uint64    `json:"id"`
}

type PagedComment struct {
	ID      uint64
	Comment Comment
}

type CommentPage struct {
	Items []PagedComment
	Next  *CommentPosition
}

// PageComments sorts by (time, stored ordinal) and resumes strictly after the
// signed position. The caller supplies one authorized, pinned thread snapshot.
func PageComments(comments []Comment, limit int, after *CommentPosition) (CommentPage, error) {
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 500 {
		return CommentPage{}, failure("invalid_params", "comment page limit must be between 1 and 500")
	}
	items := make([]PagedComment, len(comments))
	for i, comment := range comments {
		items[i] = PagedComment{ID: uint64(i) + 1, Comment: comment}
	}
	sort.Slice(items, func(i, j int) bool {
		if !items[i].Comment.Time.Equal(items[j].Comment.Time) {
			return items[i].Comment.Time.Before(items[j].Comment.Time)
		}
		return items[i].ID < items[j].ID
	})
	start := 0
	if after != nil {
		found := false
		for i, item := range items {
			if item.ID == after.ID && item.Comment.Time.Equal(after.Time) {
				start, found = i+1, true
				break
			}
		}
		if !found {
			return CommentPage{}, failure("cursor_invalid", "comment position does not match the pinned thread")
		}
	}
	end := start + limit
	if end < start || end > len(items) {
		end = len(items)
	}
	page := CommentPage{Items: append([]PagedComment{}, items[start:end]...)}
	if end < len(items) {
		last := items[end-1]
		page.Next = &CommentPosition{Time: last.Comment.Time, ID: last.ID}
	}
	return page, nil
}
