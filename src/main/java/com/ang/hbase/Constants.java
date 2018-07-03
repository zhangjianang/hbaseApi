package com.ang.hbase;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by adimn on 2018/6/25.
 */
public class Constants {
    public static final String  TABLE_NAME ="test";
    public static final String  COLUMN_FAMILY_DF ="cf1";
    public static final String  COLUMN_FAMILY_EX ="";

    public static final List<String> COMMON_TABLE = new ArrayList<String>(){{
        add("human");
        add("company");
    }};

    public static final List<String> COMPANY_CONN = new ArrayList<String>(){{
        add("annual_report");
        add("company_abnormal_info");
        add("company_category_20170411");
        add("company_category_code_20170411");
        add("company_change_info");
        add("company_check_info");
        add("company_equity_info");
        add("company_illegal_info");
        add("company_investor");
        add("company_mortgage_info");
        add("company_punishment_info");
        add("company_staff");
        add("mortgage_change_info");
        add("mortgage_pawn_info");
        add("mortgage_people_info");
    }};

    public static final List<String> REPORT_CONN = new ArrayList<String>(){{
        add("report_change_record");
        add("report_equity_change_info");
        add("report_out_guarantee_info");
        add("report_webinfo");
    }};

    public static final List<String> REPORT_S_CONN = new ArrayList<String>(){{
        add("report_outbound_investment");
        add("report_shareholder");
    }};


}
