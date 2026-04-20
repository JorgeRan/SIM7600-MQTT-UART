CC       = g++
CXXFLAGS = -Wall -O2 -std=c++17
DLIBS    = -lbcm2835 -lrt -lpthread
TARGET   = MQTT
TAIL_TEST = tail-test
OBJS     = MQTT.o ../arduPi.o ../sim7x00.o

$(TARGET): $(OBJS)
	$(CC) $(CXXFLAGS) -o $(TARGET) $(OBJS) $(DLIBS)

$(TAIL_TEST): TailReplay.cpp
	$(CC) $(CXXFLAGS) -o $(TAIL_TEST) TailReplay.cpp

MQTT.o: MQTT.cpp
	$(CC) $(CXXFLAGS) -c MQTT.cpp

../arduPi.o: ../arduPi.cpp
	$(CC) $(CXXFLAGS) -c -o $@ $<

../sim7x00.o: ../sim7x00.cpp
	$(CC) $(CXXFLAGS) -c -o $@ $<

clean:
	rm -f MQTT.o $(TARGET) $(TAIL_TEST)
