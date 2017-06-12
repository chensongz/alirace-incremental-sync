package com.zbz.zcs;

import java.util.List;
import java.util.concurrent.RecursiveAction;

/**
 * Created by zwy on 17-6-13.
 */
public class ReduceTwoToOne extends RecursiveAction {
    private List<String> fileList;

    public ReduceTwoToOne(List<String> fileList) {
        this.fileList = fileList;
    }
    @Override
    protected void compute() {

    }
}
