package com.heima;

import io.mycat.config.model.rule.RuleAlgorithm;
import io.mycat.route.function.AbstractPartitionAlgorithm;

/**
 * 实现分片的方法, 计算配置文件中的burst规则容量，分片数，步长，用于动态扩容
 */
public class HeiMaBurstRuleAlgorithm extends AbstractPartitionAlgorithm implements RuleAlgorithm {

    /**
     * volume，step，mod从配置文件中读取
     */
    //总容量约4000000000
    private Long volume;
    //步长，等于DateNode数，即每组分片数（或分表数）
    private Integer step;
    //mod，取模规则，用于确定数据存到哪个分片
    private Integer mod;

    public void setVolume(Long volume) {
        this.volume = volume;
    }

    public void setStep(Integer step) {
        this.step = step;
    }

    public void setMod(Integer mod) {
        this.mod = mod;
    }

    /**
     *
     * @param columnValue 分片id，burst字段1-2 3-2，即为id-分片id后缀
     * @return
     * burst分片
     * 分片ID = (dataId / volume) * step
     */
    @Override
    public Integer calculate(String columnValue) {
        String[] temp = columnValue.split("-");
        if(temp.length==2){
            try{
                Long dataId = Long.valueOf(temp[0]);
                Long burstId = Long.valueOf(temp[1]);
                int group = (int) (dataId / volume) * step;
                int pos = group + (int) (burstId/mod);
                System.out.println("HEIMA RULE INFO [" + columnValue + "]-[{" + pos +"}]");
                return pos;
            } catch (Exception e){
                System.out.println("HEIMA RULE INFO [" + columnValue + "]-[{" + e.getMessage() +"}]");
            }

        }
        return new Integer(0);

    }

    @Override
    public Integer[] calculateRange(String beginValue, String endValue) {
        if(beginValue != null && endValue != null){
            Integer begin = calculate(beginValue);
            Integer end = calculate(endValue);
            if(begin == null || end == null){
                return new Integer[0];
            }
            if(end >= begin){
                int len = end - begin;
                Integer [] re = new Integer[len];
                for(int i = 0; i < len; ++i){
                    re[i] = begin + i;
                }
                return re;
            }
        }
        return new Integer[0];
    }
}
