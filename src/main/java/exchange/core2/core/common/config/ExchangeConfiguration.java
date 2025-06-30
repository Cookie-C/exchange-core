package exchange.core2.core.common.config;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;


/**
 * 撮合引擎配置
 *
 * Exchange configuration
 */
@AllArgsConstructor
@Getter
@Builder
public final class ExchangeConfiguration {

    /*
     * Orders processing configuration 订单处理配置
     */
    private final OrdersProcessingConfiguration ordersProcessingCfg;

    /*
     * Performance configuration 性能配置
     */
    private final PerformanceConfiguration performanceCfg;


    /*
     * Exchange initialization configuration 交易初始化配置
     */
    private final InitialStateConfiguration initStateCfg;

    /*
     * Exchange configuration 交易配置
     */
    private final ReportsQueriesConfiguration reportsQueriesCfg;

    /*
     * Logging configuration 日志配置
     */
    private final LoggingConfiguration loggingCfg;

    /*
     * Serialization (snapshots and journaling) configuration 序列化（快照和日志）配置
     */
    private final SerializationConfiguration serializationCfg;

    @Override
    public String toString() {
        return "ExchangeConfiguration{" +
                "\n  ordersProcessingCfg=" + ordersProcessingCfg +
                "\n  performanceCfg=" + performanceCfg +
                "\n  initStateCfg=" + initStateCfg +
                "\n  reportsQueriesCfg=" + reportsQueriesCfg +
                "\n  loggingCfg=" + loggingCfg +
                "\n  serializationCfg=" + serializationCfg +
                '}';
    }

    /**
     * Sample configuration builder having predefined default settings.
     *
     * @return configuration builder
     */
    public static ExchangeConfiguration.ExchangeConfigurationBuilder defaultBuilder() {
        return ExchangeConfiguration.builder()
                .ordersProcessingCfg(OrdersProcessingConfiguration.DEFAULT)
                .initStateCfg(InitialStateConfiguration.DEFAULT)
                .performanceCfg(PerformanceConfiguration.DEFAULT)
                .reportsQueriesCfg(ReportsQueriesConfiguration.DEFAULT)
                .loggingCfg(LoggingConfiguration.DEFAULT)
                .serializationCfg(SerializationConfiguration.DEFAULT);
    }
}
