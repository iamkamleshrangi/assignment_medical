import pandas as pd
from matplotlib import pyplot as plt
from datetime import datetime, timedelta, date
import psycopg2
from pymongo import MongoClient
pd.options.mode.chained_assignment = None

class Solution(object):
    def __init__(self):
        df = pd.read_csv('../data/Surgery_Data.csv')

    def get_data(self, data):
        if isinstance(data, str):
            return data.split(' ')[0]
        return data

    def get_clean(self):
        df = pd.read_csv('../data/Surgery_Data.csv')
        print('NULL columns detected -> ', df.columns[df.isnull().sum() > 0])
        df['PREAUTH_DATE'] = pd.to_datetime(df['PREAUTH_DATE'].apply(self.get_data),format='%d/%m/%Y')
        df['CLAIM_DATE'] = pd.to_datetime(df['CLAIM_DATE'].apply(self.get_data),format='%d/%m/%Y')
        df['DISCHARGE_DATE'] = pd.to_datetime(df['DISCHARGE_DATE'].apply(self.get_data),format='%d/%m/%Y', errors = 'coerce')
        df['SURGERY_DATE'] = pd.to_datetime(df['SURGERY_DATE'].apply(self.get_data),format='%d/%m/%Y')
        df['MORTALITY_DATE'] = pd.to_datetime(df['MORTALITY_DATE'].apply(self.get_data),format='%d/%m/%Y')
        print('Format fixed for all data columns ')
        # Default Data 2000-01-01
        df['MORTALITY_DATE'].fillna(value=pd.to_datetime('2000-01-01'), inplace=True)
        df['DISCHARGE_DATE'].fillna(value=pd.to_datetime('2000-01-01'), inplace=True)
        print('null HOSP_TYPE ',df['HOSP_TYPE'].unique())
        df['HOSP_TYPE'].loc[(df['HOSP_TYPE'].isnull())] = '0'
        print('fixed HOSP_TYPE ',df['HOSP_TYPE'].unique())
        print('null SRC_REGISTRATION ', df['SRC_REGISTRATION'].unique())
        df['SRC_REGISTRATION'].loc[(df['SRC_REGISTRATION'].isnull())] = '0'
        print('fixed SRC_REGISTRATION ',df['SRC_REGISTRATION'].unique())
        print('null culumns after processing -> ', df.columns[df.isnull().sum() > 0])
        df.to_csv('../data/Surgery_Data_Processed.csv')

    def getDF(self):
        """
        Getting the clean dataFrame for the analysis
        """
        adf = pd.read_csv('../data/Surgery_Data_Processed.csv')
        adf['PREAUTH_DATE'] = pd.to_datetime(adf['PREAUTH_DATE'],format='%Y-%m-%d')
        adf['CLAIM_DATE'] = pd.to_datetime(adf['CLAIM_DATE'],format='%Y-%m-%d')
        adf['DISCHARGE_DATE'] = pd.to_datetime(adf['DISCHARGE_DATE'],format='%Y-%m-%d')
        adf['SURGERY_DATE'] = pd.to_datetime(adf['SURGERY_DATE'],format='%Y-%m-%d')
        adf['MORTALITY_DATE'] = pd.to_datetime(adf['MORTALITY_DATE'],format='%Y-%m-%d')
        adf = adf.loc[:, ~adf.columns.str.contains('^Unnamed')]
        return adf

    def question_one(self, adf):
        """
        Company wants to generate monthly report based on revenue of each
        hospital and number patients admitted across different district with
        `respect to different surgery category type for hospital financial analysis.
        """
        adf.drop(adf[adf['DISCHARGE_DATE']==pd.to_datetime('2000-01-01')].index, inplace=True)

        month_range = pd.date_range(start=adf['PREAUTH_DATE'].min(),end=adf['DISCHARGE_DATE'].max(), freq='M')
        monthly_arr = []
        for monthly in month_range:
            start_date = monthly.replace(day=1)
            end_date = monthly
            record =(adf['DISCHARGE_DATE'] > start_date) & (adf['DISCHARGE_DATE'] <= end_date)
            rdf = adf[record].groupby(['HOSP_DISTRICT','HOSP_NAME','CATEGORY_NAME']).agg({'Patient_ID':'count','CLAIM_AMOUNT':'sum'})
            if not rdf.empty:
                print('Monthly DF start ', start_date, ' End ', end_date)
                rdf['start_data'] = start_date
                rdf['end_date'] = end_date
                monthly_arr.append(rdf)
        result_df = pd.concat(monthly_arr)
        result_df = result_df.rename(columns={'Patient_ID': 'no_of_patient','CLAIM_AMOUNT':'total_claim_amount'})
        result_df.to_csv('../data/question_one.csv')
        print('question_one data save at ' + '/data/question_one.csv')

    def question_two(self, df):
        """
        Company also needs to submit report to district level hospital to show
        distribution of patient undergone surgeries of different category to know
        prevalence of disease and mortality rate in percentage based on gender.
        """
        df['Mortality_Flag'] = 0
        df['Mortality_Flag'].loc[(df['Mortality Y / N'] == 'YES')] = 1
        df['percent'] = df['Mortality_Flag']/df.groupby(['DISTRICT_NAME', 'HOSP_NAME', 'CATEGORY_NAME','SEX'])['Mortality_Flag'].transform('sum')*100
        df['percent'].loc[(df['percent'].isnull())] = 0
        tmp = df.groupby(['DISTRICT_NAME', 'HOSP_NAME', 'CATEGORY_NAME','SEX']).agg(Mortality=('percent', 'max')).sort_values('DISTRICT_NAME')
        tmp.to_csv('../data/question_two.csv')
        print('question_two data save at ' + '/data/question_two.csv')

    def question_three(self,df):
        """
        Company wants to understand duration of patient’s hospitalization stay
        and mortality rate for cardiology surgery across corporate
        and government hospital to improve quality of hospital care.
        """
        df = df[df['CATEGORY_NAME'] =='CARDIOLOGY']

        df.drop(df[df['DISCHARGE_DATE']==pd.to_datetime('2000-01-01')].index, inplace=True)
        #df.drop(df[df['MORTALITY_DATE']==pd.to_datetime('2000-01-01')].index, inplace=True)

        df['duration'] = (pd.to_datetime(df.DISCHARGE_DATE) - pd.to_datetime(df.PREAUTH_DATE)).dt.days
        #print(df[df['duration'] > 30][['DISCHARGE_DATE','PREAUTH_DATE','duration']])

        df['Mortality_Flag'] = 0
        df['Mortality_Flag'].loc[(df['Mortality Y / N'] == 'YES')] = 1
        df['percent'] = (df['Mortality_Flag']/df.groupby(['HOSP_TYPE','duration'])['Mortality_Flag'].transform('sum'))*100
        df['percent'].loc[(df['percent'].isnull())] = 0

        df  = df.groupby(['HOSP_TYPE', 'duration']).agg(Mortality=('percent', 'mean')).sort_values('HOSP_TYPE')
        #df['Mortality'].loc[(df['Mortality'].isnull())] = 0
        #print(df[['PREAUTH_DATE','DISCHARGE_DATE','duration']].head(10))
        df.to_csv('../data/question_three.csv')

    def question_four(self, df):
        """
        Company wants to identify most common source of registration through
        which patients got registered for surgeries across different hospitals
        to improve marketing strategy of hospitals
        """
        df = df['SRC_REGISTRATION'].value_counts().rename_axis('registrastion_type').reset_index(name='counts')
        df.to_csv('../data/question_four.csv')

    def question_five(self, df):
        """
        District level hospital needs to submit patient details to insurance
        company, the Medi-life company needs to generate PDF files having
        details of each patient about its tenure in hospital, claims details,
        surgery details, hospital info on daily basis.
        """
        #df['duration_in_days'] = (pd.to_datetime(df.DISCHARGE_DATE) - pd.to_datetime(df.PREAUTH_DATE)).dt.days
        df['duration_in_days'] = (pd.to_datetime(df.DISCHARGE_DATE) - pd.to_datetime(df.SURGERY_DATE)).dt.days

        df = df.groupby(['DISCHARGE_DATE','Patient_ID','HOSP_NAME','SURGERY','CLAIM_DATE']).agg({'CLAIM_AMOUNT':'sum','duration_in_days':'sum','PREAUTH_AMT':'sum'})
        df.to_csv('../data/question_five.csv')

    def question_six(self):
        """
        Company needs to store patient surgery data in secure database like SQL
        for audit trail purpose. Database auditor team want to validate the data
        based on number of surgeries in each village district wise.
        (Use Python, SQL to fulfill the requirement)
        """
        database, user, password, host, port = 'postgres','postgres','root','127.0.0.1',5432
        conn = psycopg2.connect(database=database, user=user, password=password,host=host, port=port)
        cursor = conn.cursor()
        query = '''select "DISTRICT_NAME","VILLAGE",count("SURGERY") as no_of_surgery
                    from surgery_data group by "DISTRICT_NAME","VILLAGE"'''
        cursor.execute(query)
        record = []
        for i in cursor.fetchall():
            h = {'district_name':i[0],'village':i[1],'no_of_surgery':i[2]}
            record.append(h)
        df = pd.DataFrame(record)
        df.to_csv('../data/question_six.csv')

    def get_array(self, sr):
        if isinstance(sr, int):
            return sr
        else:
            return sr.to_list()

    def question_seven(self, df):
        host = 'localhost'
        port = 27017
        dbname = 'test'
        colname = 'coltest'
        conn = MongoClient(host, port)
        db = conn[dbname]
        col = db[colname]
        for i in df['Patient_ID']:
            record_df = df[df['Patient_ID']==i]
            # Mapper
            record = dict()
            record['Patient_ID'] = i
            record['age'] = record_df['AGE'].to_list()[0]
            record['sex'] = record_df['SEX'].to_list()[0]
            record['category'] = record_df['CATEGORY_CODE'].to_list()
            record['category_name'] =record_df['CATEGORY_NAME'].to_list()
            record['surgery_code'] = record_df['SURGERY_CODE'].to_list()
            record['surgery']=  record_df['SURGERY'].to_list()
            record['village'] = record_df['VILLAGE'].to_list()[0]
            record['district_name'] = record_df['DISTRICT_NAME'].to_list()[0]
            record['perauth_date'] = record_df['PREAUTH_DATE'].to_list()[0]
            record['perauth_amt'] = record_df['PREAUTH_AMT'].to_list()[0]
            record['clain_date'] = record_df['CLAIM_DATE'].to_list()[0]
            record['claim_amt'] = record_df['CLAIM_AMOUNT'].to_list()[0]
            record['hosp_type'] = record_df['HOSP_TYPE'].to_list()[0]
            record['hosp_name'] = record_df['HOSP_NAME'].to_list()[0]
            record['hosp_location'] = record_df['HOSP_LOCATION'].to_list()[0]
            record['hosp_district']  = record_df['HOSP_DISTRICT'].to_list()[0],
            record['surgery_date'] = record_df['SURGERY_DATE'].to_list()[0]
            record['discharge_date'] = record_df['DISCHARGE_DATE'].to_list()[0]
            record['mortality_y_n'] = record_df['Mortality Y / N'].to_list()[0]
            record['mortality_date'] = record_df['MORTALITY_DATE'].to_list()[0]
            record['src_registration']= record_df['SRC_REGISTRATION'].to_list()[0]
            print(record)
            col.insert(record)

    def main(self):
        #self.get_clean()
        df = self.getDF()
        #self.question_one(df)
        #self.question_two(df)
        #self.question_three(df)
        #self.question_four(df)
        #self.question_five(df)
        #self.question_six()
        self.question_seven(df)

obj = Solution()
obj.main()
