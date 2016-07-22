package util

var dataCount int64 = 0

func Work() {

	//fixde goroutine number;
	//and start goroutine
	chs := make([]chan int, goroutineNum)

	for i := 0; i < goroutineNum; i++ {
		chs[i] = make(chan int)
		go GenerateData(chs[i])
	}

	for _, ch := range chs {
		<-ch
	}
}
