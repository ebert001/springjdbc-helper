package com.aswishes.spring;

import java.util.List;

/**
 * 分页包装器
 */
public abstract class PageResult<T> {
    /** 页码 */
    private int pageNo = 0;
    /** 每页数量 */
    private int pageSize = 20;
    /** 总页数，应当在查询完成后赋值 */
    private int pageCount = 1;
    /** 总记录数 */
    private int totalCount = 0;
    /** 下一页 */
    private int nextNo;
    /** 上一页 */
    private int prevNo;
    /** 分页数据 */
    private List<T> result;
    /** 分页数据的起始索引位置 */
    private int startIndex = 0;

    public PageResult() {}

    public PageResult(int pageSize) {
        this.pageSize = pageSize;
    }

    public PageResult(int pageNo, int pageSize) {
    	this.pageNo = pageNo;
    	this.pageSize = pageSize;
    }

    public List<T> paging() throws Exception {
        this.totalCount = queryCount();
        calPageCount();
        this.startIndex = (pageNo - 1) * pageSize;
        if (this.totalCount < 1) {
        	return null;
        }
        result = query(startIndex, pageNo, pageSize);
        return result;
    }

    public abstract int queryCount() throws Exception;

    public abstract List<T> query(int startIndex, int pageNo, int pageSize) throws Exception;

    /**
     * 查询的起始索引位置
     * @return 起始索引位置
     */
    public int getStartIndex() {
    	return this.startIndex;
    }

    /**
     * 总页数。如果总记录数为 0，页数为1.
     */
    public void calPageCount() {
        if (pageSize <= 0) {
            throw new IllegalArgumentException("每页的记录数量应当 > 0");
        }
        if (totalCount == 0) {
            pageCount = 1;
        } else if (totalCount % pageSize == 0) {
            pageCount = totalCount / pageSize;
        } else {
        	pageCount = totalCount / pageSize + 1;
        }

        if (pageNo < 1) {
        	pageNo = 1;
        }
        if (pageNo > pageCount) {
        	pageNo = pageCount;
        }
        nextNo = pageNo + 1;
        if (nextNo > pageCount) {
        	nextNo = pageCount;
        }
        prevNo = pageNo - 1;
        if (prevNo < 1) {
        	prevNo = 1;
        }
    }

    public int getPageCount() {
		return pageCount;
	}

    public List<T> getResult() {
    	return this.result;
    }

    public void setResult(List<T> list) {
    	this.result = list;
    }

    /**
     * @return 总记录数
     */
    public int getTotalCount() {
        return this.totalCount;
    }

    /**
     * @return 每页数量
     */
    public int getPageSize() {
        return this.pageSize;
    }

    /**
     * @return 当前页码
     */
    public int getPageNo() {
        return this.pageNo;
    }

    public int getPrevNo() {
    	return this.prevNo;
    }

    public int getNextNo() {
    	return this.nextNo;
    }
}
