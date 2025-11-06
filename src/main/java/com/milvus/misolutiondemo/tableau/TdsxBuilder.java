package com.milvus.misolutiondemo.tableau;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TdsxBuilder {
    private TdsBuilder tdsBuilder;
    private TdsMetadata tdsMetadata;

    public TdsxBuilder(TdsMetadata tdsMetadata) {
        this.tdsMetadata = tdsMetadata;
        this.tdsBuilder = new TdsBuilder(tdsMetadata);
    }

    public void build() {

    }
}
