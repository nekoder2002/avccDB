package dbkey

func ensureBuffer(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n)
	}
	return b[:n]
}

func shorten(str string) string {
	if len(str) <= 8 {
		return str
	}
	return str[:3] + ".." + str[len(str)-3:]
}
