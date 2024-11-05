package models

type Transaction struct {
	ID        int     `json:"id"`
	UserID    int     `json:"user_id"`
	Amount    float64 `json:"amount"`
	Type      string  `json:"type"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt string  `json:"updated_at"`
}
