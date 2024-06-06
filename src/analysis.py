from pyspark.sql.functions import col, count, desc, when, row_number,avg, lit, collect_list
from pyspark.sql import Window

class AccidentAnalysis:
    def __init__(self, data):
        self.data = data

    def analysis_1(self):
        primary_person = self.data['primary_person']
        result = primary_person.filter((col('PRSN_GNDR_ID') == 'MALE') & (col('DEATH_CNT') > 2)).count()
        return result

    def analysis_2(self):
        units = self.data['units']
        result = units.filter(col('VEH_BODY_STYL_ID').contains('MOTORCYCLE')).count()
        return result

    def analysis_3(self):
        primary_person = self.data['primary_person']
        units = self.data['units']
        merged_df = primary_person.join(units, 'CRASH_ID', "inner")\
                        .select(units.DEATH_CNT, primary_person.PRSN_AIRBAG_ID, units.VEH_MAKE_ID)
                        
        result = merged_df.filter((col('DEATH_CNT') > 0) & (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED')) \
                       .groupBy('VEH_MAKE_ID').agg(count('VEH_MAKE_ID').alias('count')) \
                       .orderBy(desc('count')).limit(5).collect()
        return result

    def analysis_4(self):
        units = self.data['units']
        primary_person = self.data['primary_person']
        merged_df = units.join(primary_person, 'CRASH_ID', "inner")
        result = merged_df.filter((col('DRVR_LIC_TYPE_ID').isin(['COMMERCIAL DRIVER LIC.','ID CARD','OCCUPATIONAL','DRIVER LICENSE'])) & (col('VEH_HNR_FL') == 'Y')).count()
        return result

    def analysis_5(self):
        primary_person = self.data['primary_person']
        result = primary_person.filter(col('PRSN_GNDR_ID') != 'FEMALE') \
                               .groupBy('DRVR_LIC_STATE_ID').agg(count('*').alias('count')) \
                               .orderBy(desc('count')).limit(1).collect()[0]['DRVR_LIC_STATE_ID']
        return result

    def analysis_6(self):
        primary_person = self.data['primary_person']
        units = self.data['units']
        units_cnt = units.withColumn("TOTAL_INJURIES",col("TOT_INJRY_CNT")+col("DEATH_CNT")) 
        result = units_cnt.groupBy('VEH_MAKE_ID').sum("TOTAL_INJURIES") \
                .select(col("VEH_MAKE_ID"),col("sum(TOTAL_INJURIES)").alias("SUM_TOTAL_INJURIES")) \
                .orderBy(desc("SUM_TOTAL_INJURIES"))
        result = result.withColumn("rn",row_number().over(Window.orderBy(desc("SUM_TOTAL_INJURIES")))) \
                .filter("rn>2 and rn<6").drop("rn").collect()
        return result

    def analysis_7(self):
        primary_person = self.data['primary_person']
        units = self.data['units']
        merged = units.join(primary_person, 'CRASH_ID', "inner")
        result = merged.groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID') \
                       .agg(count('*').alias('count'))
        result = result.withColumn("rn",row_number().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(desc("count")))) \
                .filter("rn=1").drop("rn").collect()
        return result

    def analysis_8(self):
        primary_person = self.data['primary_person']
        units = self.data['units']
        merged = units.join(primary_person, "CRASH_ID", "inner")
        result = merged.filter(col('CONTRIB_FACTR_1_ID').contains('ALCOHOL')) \
                        .groupBy('DRVR_ZIP') \
                        .agg(count('*').alias('count')) \
                        .orderBy(desc('count')).limit(5).collect()
        return result

    def analysis_9(self):
        damages = self.data['damages']
        units = self.data['units']
        merged = damages.join(units, "CRASH_ID", "inner")
        result = merged.filter((col('DAMAGED_PROPERTY').isNull()) 
                        & (col('VEH_DMAG_SCL_1_ID').contains("5") | col('VEH_DMAG_SCL_1_ID').contains("6") | col('VEH_DMAG_SCL_1_ID').contains("7")) 
                        & (col('FIN_RESP_TYPE_ID').isin(['PROOF OF LIABILITY INSURANCE','INSURANCE BINDER','LIABILITY INSURANCE POLICY','CERTIFICATE OF SELF-INSURANCE']))) \
                        .select('CRASH_ID').distinct().collect()

    def analysis_10(self):
        units = self.data['units']
        charges = self.data['charges']
        primary_person = self.data['primary_person']
        
        top_states = primary_person.filter(col("DRVR_LIC_STATE_ID").isin('NA','Unknown') == False) \
                           .groupBy('DRVR_LIC_STATE_ID') \
                           .agg(count('*').alias('count')) \
                           .orderBy(desc('count')).limit(25) \
                           .select('DRVR_LIC_STATE_ID').withColumn('RN',lit(1))
        top_states_list = top_states.groupBy("RN").agg(collect_list("DRVR_LIC_STATE_ID").alias("list")).collect()[0]["list"]

        top_colors = units.filter(col("VEH_COLOR_ID").isin('NA','Unknown') == False) \
                  .groupBy('VEH_COLOR_ID') \
                  .agg(count('*').alias('count')) \
                  .orderBy(desc('count')).limit(10) \
                  .select('VEH_COLOR_ID').withColumn('RN',lit(1))
        top_colors_list = top_colors.groupBy("RN").agg(collect_list("VEH_COLOR_ID").alias("list")).collect()[0]["list"]

        speeding_charges = charges.filter(col('CHARGE').contains('SPEED')).select('CRASH_ID').distinct()

        result = units.join(speeding_charges, 'CRASH_ID') \
               .join(primary_person, 'CRASH_ID') \
               .filter((col('DRVR_LIC_TYPE_ID').isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])) 
                       & (col('VEH_COLOR_ID').isin(top_colors_list)) 
                       & (col('DRVR_LIC_STATE_ID').isin(top_states_list))) \
               .groupBy('VEH_MAKE_ID').agg(count('*').alias('count')) \
               .orderBy(desc('count')).limit(5).collect()
        return result
