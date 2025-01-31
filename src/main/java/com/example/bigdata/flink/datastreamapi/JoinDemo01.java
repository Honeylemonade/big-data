package com.example.bigdata.flink.datastreamapi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class JoinDemo01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 构建商品数据流
        DataStream<Goods> goodsDS = env.addSource(new GoodsSource11(), TypeInformation.of(Goods.class)).assignTimestampsAndWatermarks(new MyWatermark<Goods>());
        // 构建订单明细数据流
        DataStream<OrderItem> orderItemDS = env.addSource(new OrderItemSource(), TypeInformation.of(OrderItem.class)).assignTimestampsAndWatermarks(new MyWatermark<Goods>());

        // 进行关联查询
        DataStream<FactOrderItem> factOrderItemDS = orderItemDS.join(goodsDS)
                // 第一个流orderItemDS
                .where(OrderItem::getGoodsId)
                // 第二流goodsDS
                .equalTo(Goods::getGoodsId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply((OrderItem item, Goods goods) -> {
                    FactOrderItem factOrderItem = new FactOrderItem();
                    factOrderItem.setGoodsId(goods.getGoodsId());
                    factOrderItem.setGoodsName(goods.getGoodsName());
                    factOrderItem.setCount(new BigDecimal(item.getCount()));
                    factOrderItem.setTotalMoney(goods.getGoodsPrice().multiply(new BigDecimal(item.getCount())));
                    return factOrderItem;
                });

        factOrderItemDS.print();
        env.execute("滚动窗口JOIN");
    }

    //商品类
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    public static class Goods {

        private String goodsId;
        private String goodsName;
        private BigDecimal goodsPrice;

        public static List<Goods> GOODS_LIST;
        public static Random r;

        static {
            r = new Random();
            GOODS_LIST = new ArrayList<>();
            GOODS_LIST.add(new Goods("1", "小米12", new BigDecimal(4890)));
            GOODS_LIST.add(new Goods("2", "iphone12", new BigDecimal(12000)));
            GOODS_LIST.add(new Goods("3", "MacBookPro", new BigDecimal(15000)));
            GOODS_LIST.add(new Goods("4", "Thinkpad X1", new BigDecimal(9800)));
            GOODS_LIST.add(new Goods("5", "MeiZu One", new BigDecimal(3200)));
            GOODS_LIST.add(new Goods("6", "Mate 40", new BigDecimal(6500)));
        }

        public static Goods randomGoods() {
            int rIndex = r.nextInt(GOODS_LIST.size());
            return GOODS_LIST.get(rIndex);
        }
    }

    //订单明细类
    @Data
    @ToString
    public static class OrderItem {
        private String itemId;
        private String goodsId;
        private Integer count;
    }

    //关联结果
    @Data
    @ToString
    public static class FactOrderItem {
        private String goodsId;
        private String goodsName;
        private BigDecimal count;
        private BigDecimal totalMoney;
    }

    //构建一个商品Stream源（这个好比就是维表）
    public static class GoodsSource11 extends RichSourceFunction {
        private Boolean isCancel;

        @Override
        public void open(Configuration parameters) throws Exception {
            isCancel = false;
        }

        @Override
        public void run(SourceFunction.SourceContext sourceContext) throws Exception {
            while (!isCancel) {
                Goods.GOODS_LIST.stream().forEach(goods -> sourceContext.collect(goods));
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            isCancel = true;
        }
    }

    //构建订单明细Stream源
    public static class OrderItemSource extends RichSourceFunction {
        private Boolean isCancel;
        private Random r;

        @Override
        public void open(Configuration parameters) throws Exception {
            isCancel = false;
            r = new Random();
        }

        @Override
        public void run(SourceContext sourceContext) throws Exception {
            while (!isCancel) {
                Goods goods = Goods.randomGoods();
                OrderItem orderItem = new OrderItem();
                orderItem.setGoodsId(goods.getGoodsId());
                orderItem.setCount(r.nextInt(10) + 1);
                orderItem.setItemId(UUID.randomUUID().toString());
                sourceContext.collect(orderItem);
                orderItem.setGoodsId("111");
                sourceContext.collect(orderItem);
                TimeUnit.SECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            isCancel = true;
        }
    }

    //构建水印分配器（此处为了简单），直接使用系统时间了
    public static class MyWatermark<T> implements WatermarkStrategy<T> {

        @Override
        public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (element, recordTimestamp) -> System.currentTimeMillis();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new WatermarkGenerator<T>() {
                @Override
                public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
                    output.emitWatermark(new Watermark(System.currentTimeMillis()));
                }

                @Override
                public void onPeriodicEmit(WatermarkOutput output) {
                    output.emitWatermark(new Watermark(System.currentTimeMillis()));
                }
            };
        }
    }
}
