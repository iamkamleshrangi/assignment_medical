import pandas as pd
from matplotlib import pyplot as plt
from datetime import datetime, timedelta, date
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
        respect to different surgery category type for hospital financial analysis.
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

    def main(self):
        #self.get_clean()
        df = self.getDF()
        #self.question_one(df)
        #self.question_two(df)
        #self.question_three(df)
        #self.question_four(df)
        self.question_five(df)


obj = Solution()
obj.main()
