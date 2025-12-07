"""
서울시 실시간 자전거 대여소 정보를 Kafka로 전송하는 Producer 모듈
"""
import time
import json
import logging
from confluent_kafka import Producer
from apis.seoul_data.realtime_bicycle import RealtimeBicycle
from datetime import datetime


# Kafka 브로커 서버 목록 (쉼표로 구분된 호스트:포트 형식)
BROKER_LST = 'kafka01:9092,kafka02:9092,kafka03:9092'


class BicycleProducer():
    """
    서울시 실시간 자전거 대여소 데이터를 Kafka 토픽으로 전송하는 Producer 클래스
    """

    def __init__(self, topic):
        """
        BicycleProducer 초기화
        
        Args:
            topic (str): 메시지를 전송할 Kafka 토픽 이름
        """
        self.topic = topic
        # Kafka Producer 설정 (브로커 서버 목록)
        self.conf = {'bootstrap.servers': BROKER_LST
                      ,'compression.type': 'lz4'
                      , 'enable.idempotence': True
                      , 'max.in.flight.requests.per.connection': 5
                      , 'acks': 'all'}
        self.producer = Producer(self.conf)
        self._set_logger()

    def _set_logger(self):
        """
        로깅 설정 초기화
        로그 포맷: [날짜 시간] [레벨]:메시지
        """
        logging.basicConfig(
            format='%(asctime)s [%(levelname)s]:%(message)s',
            level=logging.INFO,
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        self.log = logging.getLogger(__name__)

    def delivery_callback(self, err, msg):
        """
        메시지 전송 결과를 처리하는 콜백 함수
        poll() 또는 flush() 호출 시 메시지 전송 성공/실패 시 트리거됨
        
        Args:
            err: 전송 오류 객체 (None이면 성공)
            msg: 전송된 메시지 객체
        """
        if err:
            self.log.error('%% Message failed delivery: %s\n' % err)
        else:
            # 전송 성공 시 별도 로깅 없음 (성능 고려)
            pass

    def produce(self):
        """
        서울시 실시간 자전거 대여소 데이터를 조회하여 Kafka로 전송
        15초 간격으로 반복 실행
        """
        # 서울시 실시간 자전거 데이터 API 클라이언트 초기화
        rt_bycicle = RealtimeBicycle(dataset_nm='bikeList')
        
        while True:
            # 현재 시간을 문자열로 변환 (데이터 생성 시각 기록용)
            now_dt = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # API를 통해 실시간 자전거 대여소 데이터 조회
            items = rt_bycicle.call()
            
            # 각 대여소별로 데이터 처리
            for item in items:
                # API 응답의 영문 컬럼명을 한글 약어로 변경
                item['STT_ID'] = item.pop('stationId')  # 대여소 ID
                item['STT_NM'] = item.pop('stationName')  # 대여소명
                item['TOT_RACK_CNT'] = item.pop('rackTotCnt')  # 거치대 총 개수
                item['TOT_PRK_CNT'] = item.pop('parkingBikeTotCnt')  # 주차된 자전거 총 개수
                item['RATIO_PRK_RACK'] = item.pop('shared')  # 거치율
                item['STT_LTTD'] = item.pop('stationLatitude')  # 대여소 위도
                item['STT_LGTD'] = item.pop('stationLongitude')  # 대여소 경도

                # 데이터 생성 시각 컬럼 추가
                item['CRT_DTTM'] = now_dt

                # Kafka로 메시지 전송
                try:
                    self.producer.produce(
                        topic=self.topic,  # 대상 토픽
                        # 메시지 키: 대여소 ID와 생성 시각 조합 (파티셔닝 및 중복 방지용)
                        key=json.dumps({'STT_ID': item['STT_ID'],'CRT_DTTM':item['CRT_DTTM']}, ensure_ascii=False),
                        # 메시지 값: 전체 데이터 (JSON 형식)
                        value=json.dumps(item, ensure_ascii=False),
                        # 전송 결과 콜백 함수
                        on_delivery=self.delivery_callback
                    )

                except BufferError:
                    # Producer 내부 큐가 가득 찬 경우 (전송 속도가 너무 빠를 때)
                    self.log.error('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                                     len(self.producer))

            # 비동기 전송된 메시지들의 전송 결과 콜백 처리
            # 주의: produce()는 비동기 API이므로 poll() 호출 시 마지막 메시지의 콜백은
            #       아직 처리되지 않을 수 있음
            self.producer.poll(0)

            # 모든 메시지가 전송될 때까지 대기
            self.log.info('%% Waiting for %d deliveries\n' % len(self.producer))
            self.producer.flush()

            # 다음 데이터 조회까지 15초 대기
            time.sleep(15)


if __name__ == '__main__':
    # 스크립트 직접 실행 시: 자전거 데이터 Producer 시작
    bicycle_producer = BicycleProducer(topic='apis.seouldata.rt-bicycle')
    bicycle_producer.produce()