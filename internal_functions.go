package producer

func stringPtrToString(str *string) string {
	if str == nil {
		return "<nil>"
	}
	return string(*str)
}
