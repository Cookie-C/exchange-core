/*
 * Copyright 2019 Maksim Zheravin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package exchange.core2.core.common;


import lombok.*;
import net.openhft.chronicle.bytes.BytesIn;
import net.openhft.chronicle.bytes.BytesOut;
import net.openhft.chronicle.bytes.WriteBytesMarshallable;

import java.util.Objects;

/**
 * 核心币种规范
 */
@Builder
@AllArgsConstructor
@Getter
@ToString
public final class CoreSymbolSpecification implements WriteBytesMarshallable, StateHash {

    /**
     * 标志ID
     */
    public final int symbolId;

    /**
     * 标志类型
     */
    @NonNull
    public final SymbolType type;

    // currency pair specification 货币对规格
    public final int baseCurrency;  // base currency 基础货币
    public final int quoteCurrency; // quote/counter currency (OR futures contract currency) 报价货币/对冲货币
    public final long baseScaleK;   // base currency amount multiplier (lot size in base currency units) 基础货币金额乘数-汇率(以基础货币单位表示的交易量)
    public final long quoteScaleK;  // quote currency amount multiplier (step size in quote currency units) 报价货币金额乘数-汇率（）

    // fees per lot in quote? currency units 报价中每批的费用？货币单位?
    // 取款手续费
    public final long takerFee; // TODO check invariant: taker fee is not less than maker fee
    // 市场手续费
    public final long makerFee;

    // 边距设置 仅适用于期货合约
    // margin settings (for type=FUTURES_CONTRACT only)
    public final long marginBuy;   // buy margin (quote currency) 购买保证金
    public final long marginSell;  // sell margin (quote currency) 卖出保证金

    public CoreSymbolSpecification(BytesIn bytes) {
        this.symbolId = bytes.readInt();
        this.type = SymbolType.of(bytes.readByte());
        this.baseCurrency = bytes.readInt();
        this.quoteCurrency = bytes.readInt();
        this.baseScaleK = bytes.readLong();
        this.quoteScaleK = bytes.readLong();
        this.takerFee = bytes.readLong();
        this.makerFee = bytes.readLong();
        this.marginBuy = bytes.readLong();
        this.marginSell = bytes.readLong();
    }

/* NOT SUPPORTED YET:

//    order book limits -- for FUTURES only
//    public final long highLimit;
//    public final long lowLimit;

//    swaps -- not by
//    public final long longSwap;
//    public final long shortSwap;

// activity (inactive, active, expired)

  */

    @Override
    public void writeMarshallable(BytesOut bytes) {
        bytes.writeInt(symbolId);
        bytes.writeByte(type.getCode());
        bytes.writeInt(baseCurrency);
        bytes.writeInt(quoteCurrency);
        bytes.writeLong(baseScaleK);
        bytes.writeLong(quoteScaleK);
        bytes.writeLong(takerFee);
        bytes.writeLong(makerFee);
        bytes.writeLong(marginBuy);
        bytes.writeLong(marginSell);
    }

    @Override
    public int stateHash() {
        return Objects.hash(
                symbolId,
                type.getCode(),
                baseCurrency,
                quoteCurrency,
                baseScaleK,
                quoteScaleK,
                takerFee,
                makerFee,
                marginBuy,
                marginSell);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CoreSymbolSpecification that = (CoreSymbolSpecification) o;
        return symbolId == that.symbolId &&
                baseCurrency == that.baseCurrency &&
                quoteCurrency == that.quoteCurrency &&
                baseScaleK == that.baseScaleK &&
                quoteScaleK == that.quoteScaleK &&
                takerFee == that.takerFee &&
                makerFee == that.makerFee &&
                marginBuy == that.marginBuy &&
                marginSell == that.marginSell &&
                type == that.type;
    }
}
