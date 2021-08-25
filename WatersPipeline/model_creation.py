
import numpy as np
import pandas as pd

from datetime import date
from functools import reduce
from statistics import median
# HIDING AND MAGIC COMMANDS
import warnings
warnings.simplefilter("ignore")



class GenerateData():
  
    def __init__(self, data,august_date):
      
        # Importing libraries.
        
        # Copying Data
        copied_data = data.copy()
        self.august_date = august_date
      
        self.copied_data = copied_data
      
    def singleOrder(self):
        """
        Description: This method will create a column which tells us whether a customer has a single order or not.
        
        Input Parameters:
        ----------------
        Takes in the data.
        
        Output:
        ------
        Returns a Dataframe with two columns, one being unique account ID and the other column indicating whether a single
        purchase was made or not.
        """
        
        unique_userid = list(self.copied_data['accountsapid'].unique())
        grouped_data = self.copied_data.groupby('accountsapid')
        unique_users_df = pd.DataFrame({'accountsapid':[],'Single_Order':[]})#'Quarter_avg':[], 'Year_avg':[]})
       
        for ids in unique_userid:
            try:
                current_user_group = grouped_data.get_group(ids)
                if (current_user_group['OrderCreateDate'].nunique() == 1):
                    current_id_dict = {'accountsapid':ids,'Single_Order':1}
                    unique_users_df = unique_users_df.append([current_id_dict])
                else:
                    current_id_dict = {'accountsapid':ids,'Single_Order':0}
                    unique_users_df = unique_users_df.append([current_id_dict])
            except Exception as exp:
                print('You have got',type(exp))
                
                
        return unique_users_df
  
  
    def makeYears(self):
        """
        Description: This method will Create a column which tells customer has churned in which year 
        
        Input Parameters:
        -----------------
        Takes in the data
        
        Output:
        ------
        Returns a DataFrame with 4 columns, one being unique account ID and the other columns are year columns with
        binary mapping of 0 and 1  which shows whether Whether the purchase by Customer is made by that customer in 
        year or not 
        
        """
        unique_userid = list(self.copied_data['accountsapid'].unique())
        self.copied_data['OrderCreateDate'] = pd.to_datetime(self.copied_data['OrderCreateDate'], format='%Y-%m-%d')
        self.copied_data['Years'] = self.copied_data['OrderCreateDate'].dt.year
        
        grouped_data = self.copied_data.groupby('accountsapid')
        single_order_df = self.singleOrder()
        single_order_df = single_order_df.reset_index(drop=True)
        
        for ids in unique_userid: 
            try:
                for year in grouped_data.get_group(ids)['Years'].unique():
                    indexes=single_order_df[single_order_df['accountsapid']==ids].index
                    single_order_df.loc[indexes[0],str(year)]=1
            except Exception as exp:
                print('You have got',type(exp))
                
        single_order_df = single_order_df.fillna(0)
       
        return single_order_df
  
  
    def ruleOneTarget(self, cols):
        """
        Description: This method will return Whether Customer with single transaction Churned or not  
        
        Input Parameters:
        -----------------
        takes in data , Data having Singe_Order and Year columns 
        
        Output:
        ------
        Return  Whether Customer churned or not based on rules :-refer to document for Rules of single 
        Transaction Customers
        """
        # Single_Order, 2019, 2020, 2021
        if cols[0]==1:
            if cols[3]==1:
                return 'WillNot_Churn'
            else:
                return 'Will_Churn'
        else:
            if (cols[1],cols[2],cols[3])==(1,1,1):
                return 'WillNot_Churn'
            elif (cols[1],cols[2],cols[3])==(1,0,0):
                return 'Will_Churn'
            elif (cols[1],cols[2],cols[3])==(0,1,0):
                return 'Will_Churn'
            elif (cols[1],cols[2],cols[3])==(0,0,1):
                return 'WillNot_Churn'
            elif (cols[1],cols[2],cols[3])==(1,1,0):
                return 'Will_Churn'
            elif (cols[1],cols[2],cols[3])==(1,0,1):
                return 'Will_Churn'
            elif (cols[1],cols[2],cols[3])==(0,1,1):
                return 'WillNot_Churn'     
  
  
    def ruleOne(self):
        """
        Description: This method will return DataFrame which contain Single Order , years column,churn
        rule year wise 
        
        
        Input Parameters:
        -----------------
        Takes in the data
        
        Output:
        ------
        Returns a DataFrame with 5 columns, one being unique account ID and the other columns are year columns with
        binary mapping of 0 and 1  which shows whether Whether the purchase by Customer is made by that customer in 
        year or not . Last column will be of Churn Rule that will tell whether customer churned or not
        
        """
        
        single_order_year_df = self.makeYears()
        single_order_year_df['Churn_Rule_year_wise']=single_order_year_df[['Single_Order','2019','2020','2021']].apply(self.ruleOneTarget, axis=1)
        return single_order_year_df
  
    def medianRule(self, all_dates):
        """
        Description: This method will check the avg Purchase cycle of customer and its rule :- Refer to document 
        
        Input Parameters:
        -----------------
        Takes in the data , Unique Transactions Dates of Customers 
        
        Output:
        ------
        Return Whether Cutomer churn or not based on churn avg cycle rule and will also return its median
        and 3 year avg  median(3*median)
        
        """
        try:
            all_dates = pd.Series(all_dates)
            median_value = all_dates - all_dates.shift(1)
            median_value = median(median_value[1:])
            todays_date = date(2021,6,28)
            days_difference = (todays_date - date(all_dates[-1:].dt.year.values[0],
                                                       all_dates[-1:].dt.month.values[0],
                                                       all_dates[-1:].dt.day.values[0])    ).days


            median_value = round(median_value.days)
        except Exception as exp:
                print('You have got',type(exp))
        if median_value*3 >= days_difference:
            return ['WillNot_Churn', median_value , median_value*3]
        else:
            return ['Will_Churn', median_value , median_value*3]
        
    def medianRuleAug(self, all_dates):
        """
        Description: This method will check the avg Purchase cycle of customer and Whether Customer is churning or not 
        wrt to august month 
        
        Input Parameters:
        -----------------
        Takes in the data , Unique Transactions Dates of Customers 
        
        Output:
        ------
        Return Whether Cutomer churn or not based on churn avg cycle rule and will also return its median
        and 3 year avg  median(3*median)
        
        """
        try:
            all_dates = pd.Series(all_dates)
            median_value = all_dates - all_dates.shift(1)
            median_value = median(median_value[1:])
            aug_date =date(2021,8,28)
            aug_days_difference = (aug_date - date(all_dates[-1:].dt.year.values[0],
                                                       all_dates[-1:].dt.month.values[0],
                                                       all_dates[-1:].dt.day.values[0])    ).days

            median_value = round(median_value.days)
        except Exception as exp:
                print('You have got',type(exp))
        if median_value*3 >= aug_days_difference:
            return ['WillNot_Churn', median_value , median_value*3]
        else:
            return ['Will_Churn', median_value , median_value*3]
        
  
    def ruleTwo(self):
        """
        Description: This method is for that Customers who has Multiple Transactions and will check its year wise rule 
        and avg purchase cycle rule For table refer to Docs
        
        Input Parameters:
        -----------------
        Takes in the data
        
        Output:
        ------
        Returns a DataFrame with 10 columns, one being unique account ID and three of the other columns are year columns with
        binary mapping of 0 and 1  which shows whether Whether the purchase by Customer is made by that customer in 
        year or not , one is Median Days which shows the median between the transactions , Other is 3_years_cycle which 
        is median*3 shows 3 year cycle of user , last column is ChurnRule Year wise which tells whether customer will churn
        or not , Same 3 columns for august date 
        
        """
       
        single_order_year_df = self.makeYears()
        ruleOne_out = self.ruleOne()
        unique_userid = list(self.copied_data['accountsapid'].unique())
        grouped_data = self.copied_data.groupby('accountsapid')
        unique_users_df = pd.DataFrame({'accountsapid':[],'Churn_Rule_avg_cycle':[],
                                        'Median_Days':[],'3_years_cycle':[]})
        # making separate dataframe for august date 28/08/2021 
        if self.august_date == True:
            unique_augusers_df = pd.DataFrame({'accountsapid':[],'Churn_Rule_avg_cycle_aug':[],
                                            'Median_Days_Aug':[],'3_years_cycle_aug':[]})

        for ids in unique_userid:
            try:
                if single_order_year_df[single_order_year_df['accountsapid']==ids]['Single_Order'].values[0]==1:
                    current_user_dict = {'accountsapid':ids, 'Churn_Rule_avg_cycle':-1,
                                         'Median_Days':0,'3_years_cycle':0}

                    #Remove below two line of don't want august users 
                    aug_user_dict = {'accountsapid':ids, 'Churn_Rule_avg_cycle_aug':-1,
                                         'Median_Days_Aug':0,'3_years_cycle_aug':0}
                    if self.august_date==True:
                        unique_augusers_df=unique_augusers_df.append([aug_user_dict])
                    unique_users_df=unique_users_df.append([current_user_dict])
                else:
                    all_dates = sorted(list(grouped_data.get_group(ids)['OrderCreateDate'].unique()))
                    median_output = self.medianRule(all_dates)


                    current_user_dict = {'accountsapid':ids, 'Churn_Rule_avg_cycle':median_output[0],
                                         'Median_Days':median_output[1],'3_years_cycle':median_output[2]}


                    if self.august_date==True:
                        medianAug_output = self.medianRuleAug(all_dates)
                        aug_user_dict = {'accountsapid':ids, 'Churn_Rule_avg_cycle_aug':medianAug_output[0],
                                             'Median_Days_Aug':medianAug_output[1],'3_years_cycle_aug':medianAug_output[2]}

                        unique_augusers_df=unique_augusers_df.append([aug_user_dict])

                    unique_users_df=unique_users_df.append([current_user_dict])
            except Exception as exp:
                print('You have got',type(exp))
        if self.august_date==True:
            JuneAug_users_df = pd.merge(left=unique_augusers_df, right=unique_users_df, on='accountsapid')
            final_dataset = pd.merge(left=ruleOne_out, right=JuneAug_users_df, on='accountsapid')
        else:
            final_dataset = pd.merge(left=ruleOne_out, right=unique_users_df, on='accountsapid')
            
        return final_dataset
    
    def make_final_churn(self,cols):
        """
        Description: This method will See avg cycle and year cycle rule 
        
        Input Parameters:
        -----------------
        Takes in the data, Avg cycle and year wise cycle rules
        
        Output:
        ------
        Returns whether Customer churnor not  
        
        """
        if cols[1]==-1:
            return cols[0]
        elif tuple(cols)==('WillNot_Churn','WillNot_Churn'):
            return 'WillNot_Churn'
        elif tuple(cols)==('WillNot_Churn','Will_Churn'):
            return 'Will_Churn'
        elif tuple(cols)==('Will_Churn','WillNot_Churn'):
            return 'WillNot_Churn'
        elif tuple(cols)==('Will_Churn','Will_Churn'):
            return 'Will_Churn'
    
    def finalChurn(self):
        """
        Description: This method will create Final Dataset which will have ruleOne and ruleTwo outputs
        
        Input Parameters:
        -----------------
        Takes in the data
        
        Output:
        ------
        Returns a DataFrame with 13 columns, one being unique account ID and three of the other columns are year columns with
        binary mapping of 0 and 1  which shows whether Whether the purchase by Customer is made by that customer in 
        year or not , one is Median Days which shows the median between the transactions , Other is 3_years_cycle which 
        is median*3 shows 3 year cycle of user , one column is ChurnRule Year wise which tells whether customer will churn
        or not on year wise , last column is final rule which tells whether customer churn or not, Same 4 column of august date 
    
        """
        final_dataset = self.ruleTwo()
        final_dataset['Final_Churn']=final_dataset[['Churn_Rule_year_wise','Churn_Rule_avg_cycle']].apply(self.make_final_churn,axis=1)
        if self.august_date==True:
                final_dataset['Final_Churn_Aug']=final_dataset[['Churn_Rule_year_wise','Churn_Rule_avg_cycle_aug']].apply(self.make_final_churn,axis=1)
        return final_dataset


import numpy as np
import pandas as pd

from datetime import date
from functools import reduce
from statistics import median

        
import warnings
#warnings.simplefilter("ignore")



# HIDING AND MAGIC COMMANDS
import warnings
warnings.simplefilter("ignore")




class DataPreprocessing():
  
    def __init__(self, data):
      
        # Copying Data
        copied_data = data.copy()
      
        self.copied_data = copied_data
    
    def CreateFirstLastDate(self):
        
        """
        Description: This method will create first and last date of Customer
        
        Input Parameters:
        -----------------
        Takes in the data
        
        Output:
        ------
        Returns a dataframe  First Purchase date and last purchase date of Customer along with unique account id  in a dataframe 

        """
        grouped_data = self.copied_data.groupby('accountsapid')
        uniqueIds =  self.copied_data['accountsapid'].unique()
        dates_df = pd.DataFrame({'accountsapid':[],'FirstOrderedDate':[],'LastOrderedDate':[]})
        
        
        for ids in uniqueIds:
            try:
                sorted_dates = sorted(list(grouped_data.get_group(ids)['OrderCreateDate']))
                if len(sorted_dates)==1:
                    current_user_date = {'accountsapid':ids ,'FirstOrderedDate':sorted_dates[0],'LastOrderedDate':sorted_dates[0]}
                    dates_df=dates_df.append([current_user_date])

                else:
                    current_user_date = {'accountsapid':ids ,'FirstOrderedDate':sorted_dates[0],'LastOrderedDate':sorted_dates[-1]}
                    dates_df=dates_df.append([current_user_date])
            except Exception as exp:
                print('You have got',type(exp))
            
        return dates_df

    
    def NumQtrsPerOrder(self):
        """
        Description: This method will Calculate Qtr Average cycle of all customer transactions 
        
        Input Parameters:
        -----------------
        Takes in the data
        
        Output:
        ------
        Returns a dataframe having  unique Account Id and Qtr average cycle column of Customer 
         
        
        """
        df = self.copied_data
        grouped_data = df.groupby('accountsapid')
        uniqueIds =  df['accountsapid'].unique()
        quarter_df = pd.DataFrame({'accountsapid':[],'NumQtrsPerOrder':[]})
        
        
        df['quarter'] = df['OrderCreateDate'].dt.quarter

        for ids in uniqueIds:
            try:
                current_user_data = grouped_data.get_group(ids)
                current_user_data['quarter'] = current_user_data['OrderCreateDate'].dt.quarter

                qtr_list = []
                for year in sorted(list(current_user_data['OrderCreateDate'].dt.year.unique())):
                    if year ==2019:
                        num_of_years =0
                    elif year==2020:
                        num_of_years =1
                    else:
                        num_of_years =2                   


                    YearQtr= sorted(list(current_user_data[current_user_data['OrderCreateDate'].dt.year==year]['quarter'].unique()))
                    YearQtr =[(4*num_of_years) + qtrs for qtrs in YearQtr]
                    qtr_list.append(YearQtr)



                qtr_series=pd.Series(sorted(sum(qtr_list,[])))
                if len(qtr_series) ==1:
                    current_user_qtr = {'accountsapid':ids,
                                        'NumQtrsPerOrder' :-1 }  
                    quarter_df = quarter_df.append([current_user_qtr])
                else:
                    qtr_sum = sum((qtr_series - qtr_series.shift(1))[1:])         
                    current_user_qtr = {'accountsapid':ids,
                                        'NumQtrsPerOrder' :round(qtr_sum/(len(qtr_series)-1)) }  
                    quarter_df = quarter_df.append([current_user_qtr])
            except Exception as exp:
                print('You have got',type(exp))
        return quarter_df  
    
    
    def NumYearsOfUser(self,total_years):
        """
        Description: This method will create yearly average cycle of customers  
        
        Input Parameters:
        -----------------
        Takes in the data, Total Years in data 
        
        Output:
        ------
        Returns a dataframe having unique Account Id and year average cycle of user 
        
        
        """
        grouped_data = self.copied_data.groupby('accountsapid')
        uniqueIds =  self.copied_data['accountsapid'].unique()
        
        year_df = pd.DataFrame({'accountsapid':[],
                                'Average_years_taken_per order':[]})
        
        for ids in uniqueIds:
            try:
                current_user_data = grouped_data.get_group(ids)
                current_user_qtr = {'accountsapid':ids,
                                    'Average_years_taken_per order' :round(total_years/len(current_user_data['OrderCreateDate'].dt.year.unique())) }  
                year_df = year_df.append([current_user_qtr])
            except Exception as exp:
                print('You have got',type(exp))
            
        return year_df
    
    
    def RScore(self,recency_value,recency,DictRfm):
        """
        Description: This method will Calculate the Recency score of Customer wrt quantiles
        
        Input Parameters:
        -----------------
        Takes in the data, recency value of customer , recency ,dictionary having keys as 
        recency , frequency,monetary, and their values as quantile range    
        
        Output:
        ------
        Will return numeric value range between 1 to 4 denoting score of recency with 
        1 being good and 4 being worst 
        """
        if recency_value <= DictRfm[recency][0.25]:
            return 1
        elif recency_value <= DictRfm[recency][0.50]:
            return 2
        elif recency_value <= DictRfm[recency][0.75]: 
            return 3
        else:
            return 4
    
    def FMScore(self,FreqMon_value,FreqMon,DictRfm):
        """
        Description: This method will Calculate  Frequency and Monetary score of Customer wrt quantiles
        
        Input Parameters:
        -----------------
        Takes in the data, freq or monetary value of customer , frequency/monetray ,dictionary having keys as 
        recency , frequency,monetary, and their values as quantile range
        Output:
        ------
        Will return numericc value range between 1 to 4 denoting score of frequency, Monetary with 
        1 being good and 4 being worst 
        
        """
        if FreqMon_value <= DictRfm[FreqMon][0.25]:
            return 4
        elif FreqMon_value <= DictRfm[FreqMon][0.50]:
            return 3
        elif FreqMon_value <= DictRfm[FreqMon][0.75]: 
            return 2
        else:
            return 1
        
    def RfmScore(self):
        """
        Description: This method will Call RScore and FM score and calculate their respective values 
        
        Input Parameters:
        -----------------
        Takes in the data,
        
        Output:
        ------
        Returns a dataframe having unique accounts id ,RFM  Score with 3 being ideal customers and 12 being
        worst customers
        
        """
        rfm_dataset = self.RecencyFreqmonetary()
        quantiles = rfm_dataset.iloc[:,1:].quantile(q=[0.25,0.50,0.75])
        quantiles = quantiles.to_dict()   
        
        try:
            segmented_rfm = rfm_dataset.iloc[:,1:].copy()

            segmented_rfm['R_quartile'] = segmented_rfm['Recency'].apply(self.RScore, args=('Recency',quantiles))
            segmented_rfm['F_quartile'] = segmented_rfm['Frequency'].apply(self.FMScore, args=('Frequency',quantiles))
            segmented_rfm['M_quartile'] = segmented_rfm['Monetary'].apply(self.FMScore, args=('Monetary',quantiles))

            segmented_rfm['RFM_Segment'] = segmented_rfm.R_quartile.map(str)+segmented_rfm.F_quartile.map(str)+segmented_rfm.M_quartile.map(str)
            segmented_rfm['RFM_Score'] = segmented_rfm[['R_quartile','F_quartile','M_quartile']].sum(axis=1)
        except Exception as exp:
                print('You have got',type(exp))
        rfm_dataset = pd.DataFrame({'accountsapid':rfm_dataset['accountsapid'],
                                    'RFM_Score':segmented_rfm['RFM_Score']})
        return rfm_dataset
        
        
        
    
    def RecencyFreqmonetary(self):
        
        """
        Description: This method will Calculate Recency , Monetary and Frequency Value of Customers 
        
        Input Parameters:
        -----------------
        Takes in the data,
        
        Output:
        ------
        Returns a dataframe having unique account id , recency monetaryu,frequency score of Customers 
        
        """
        
        grouped_data = self.copied_data.groupby('accountsapid')
        uniqueIds =  self.copied_data['accountsapid'].unique()
        rfm_df = pd.DataFrame({'accountsapid':[],'Recency':[],
                               'Frequency':[],'Monetary':[]})
        
        for ids in uniqueIds:
                try:
                    current_user_data = grouped_data.get_group(ids).sort_values('OrderCreateDate')

                    last_purchase_date = date(current_user_data['OrderCreateDate'].dt.year.values[0],
                                              current_user_data['OrderCreateDate'].dt.month.values[0],
                                              current_user_data['OrderCreateDate'].dt.day.values[0])
                    today_date = date(2021 , 6, 28)                
                    recency = (today_date - last_purchase_date).days

                    frequency = sum(current_user_data['OrderQuantity'].values)

                    monetary = sum(current_user_data['OrderAmount(USD)'].values)

                    current_user = {'accountsapid':ids,'Recency':recency ,
                                    'Frequency':frequency,'Monetary':monetary}
                    rfm_df= rfm_df.append([current_user])
                except Exception as exp:
                    print('You have got',type(exp))
        return rfm_df
    
    
    def OrderNumberAccount(self):
        """
        Description: This method will Calculate Total Number of Order customer has made in 
        
        Input Parameters:
        -----------------
        Takes in the data,
        
        Output:
        ------
        Returns a dataframe having unique account id and Order number of Customers 
        
        """
        
        grouped_data = self.copied_data.groupby('accountsapid')
        uniqueIds =  self.copied_data['accountsapid'].unique()
        ordernumber_df = pd.DataFrame({'accountsapid':[],'TotalOrderNumber':[],})
        
        for ids in uniqueIds:
            try:
                current_user_data = grouped_data.get_group(ids)
                current_user = {'accountsapid':ids,'TotalOrderNumber':len(current_user_data['ordernumber'].unique())}
                ordernumber_df=ordernumber_df.append([current_user])
            except Exception as exp:
                print('You have got',type(exp))
        return ordernumber_df
    
    
    def CountryCodeUser(self):
        """
        Description: This method will Calculate each Country for Customers in which the Account Associated
        
        Input Parameters:
        -----------------
        Takes in the data,
        
        Output:
        ------
        Returns a dataframe having unique Account id and Its Countrycode eg. US, CN, PR
        
        """
        grouped_data = self.copied_data.groupby('accountsapid')
        uniqueIds =  self.copied_data['accountsapid'].unique()
        CountryCode_df = pd.DataFrame({'accountsapid':[],'CountryCode':[],})
        
        for ids in uniqueIds:
            try:
                current_user_data = grouped_data.get_group(ids)
                current_user = {'accountsapid':ids,'CountryCode':current_user_data['SalesforceAccountCountryCode'].values[0]}
                CountryCode_df=CountryCode_df.append([current_user])
            except Exception as exp:
                print('You have got',type(exp))
            
        return CountryCode_df
    
    
    
                
        
    
    def OrderChannelUser(self):
        """
        Description: This method will assign the unique channel of the customer from where transactions have been made
        
        
        Input Parameters:
        -----------------
        Takes in the data,
        
        Output:
        ------
        Returns a dataframe having Unique Account Id and Channel from which customers have made transactions this could be multiple 
        eg. NonDigital ,EProcurement, Ecommmerce
        
        """
        
        grouped_data = self.copied_data.groupby('accountsapid')
        uniqueIds =  self.copied_data['accountsapid'].unique()
        OrderChannel_df = pd.DataFrame({'accountsapid':[],'OrderChannel_NonDigital':[],
                                       'OrderChannel_EProcurement':[],'OrderChannel_ECommerce':[]})
        
        for ids in uniqueIds:
            try:
                current_user_data = grouped_data.get_group(ids)
                OneHotEncoded_df = current_user_data['OrderChannel'].unique()
                digital=0
                procurement=0
                ecommerce=0
                for channel in OneHotEncoded_df:
                    if channel=='NonDigital':
                        digital = 1
                    elif channel =='EProcurement':
                        procurement =1
                    else:
                        ecommerce =1

                current_user = {'accountsapid':ids,'OrderChannel_NonDigital':digital,
                               'OrderChannel_EProcurement':procurement,'OrderChannel_ECommerce':ecommerce}
                OrderChannel_df=OrderChannel_df.append([current_user])
            except Exception as exp:
                print('You have got',type(exp))
            
        return OrderChannel_df
        
        
        
    
    def MergingAllDataSet(self):
        """
        Description: This method will merge all dataset that we have created so far 
        
        Input Parameters:
        -----------------
        Takes in the data,
        
        Output:
        ------
        Returns a dataframe having  14 columns starting with uniquq Account id , first and last order date having 
        format '%Y-%m-%d' , Total order number ,qtr and year purchase cycle ,3 columns of channel(Nondigital,
        Eprocurement,Ecommerce) , recency,monetary,frequency and rfm score , last being Country code 
        
        """
        ordernumber_df = self.OrderNumberAccount()
        CreateDate_df = self.CreateFirstLastDate()        
        QtrData_df =    self.NumQtrsPerOrder()
        YearAvg_df =   self.NumYearsOfUser(len(self.copied_data['OrderCreateDate'].dt.year.unique()))   
        RfmScore_df =   self.RfmScore()
        rfmValue_df =  self.RecencyFreqmonetary()
        CountryCode_df = self.CountryCodeUser()
        OrderChannel_df = self.OrderChannelUser()
        
        final_dataset = [ordernumber_df, CreateDate_df, QtrData_df, YearAvg_df,
                         RfmScore_df,rfmValue_df,CountryCode_df,OrderChannel_df]
        df_final = reduce(lambda left,right: pd.merge(left,right,on='accountsapid'), final_dataset)


        
        
        return df_final

                
# HIDING AND MAGIC COMMANDS
import warnings
warnings.simplefilter("ignore")


import pandas as pd



class DataCleaning():
  
    def __init__(self, data):
      
        # Copying Data
        copied_data = data.copy()
      
        self.copied_data = copied_data
    def UserRules(self):
        df = self.copied_data
        final_churn = GenerateData(df,august_date=False).finalChurn()
        return final_churn
    
    def FeatureEngineering(self):
        df = self.copied_data
        data_gen_pre = DataPreprocessing(df).MergingAllDataSet()
        return data_gen_pre
        
    def FinalData(self):
        return pd.merge(left= self.UserRules(), right=self.FeatureEngineering(),on='accountsapid')
        
        

import numpy as np
import pandas as pd

from datetime import date

     


# MODELLING
from sklearn.linear_model import LogisticRegression



# DATA VALIDATION AND METRIC.
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score, KFold
from sklearn.metrics import recall_score
from sklearn.feature_selection import SelectKBest,f_classif


# HIDING AND MAGIC COMMANDS
import warnings
warnings.simplefilter("ignore")




class Model():
    def __init__(self,data):
        copied_data=data.copy()
        self.copied_data = copied_data
    
    def EDA_Data(self):
        df = self.copied_data
        date_column = list(df.columns[1:4])
        for index,col_value in df[date_column].iterrows():
            year_availability= dict(zip(col_value.index,col_value.values))
            sent= 'Orders in '
            for years in year_availability:
                if year_availability[years]==1:

                    sent= sent+years+','
                    df.loc[index,years]='Yes'
                else:
                    df.loc[index,years]='No'
            df.loc[index,'Orders year wise']=sent[:-1]

        for index,col_value in df[['OrderChannel_NonDigital','OrderChannel_EProcurement','OrderChannel_ECommerce']].iterrows():
            channel_availability= dict(zip(col_value.index,col_value.values))
            for channel in channel_availability:
                if channel_availability[channel]==1:
                    df.loc[index,channel]='Yes'
                else:
                    df.loc[index,channel]='No'

        return df
    
    ## Handling dates
    def handle_dates(self,df_new,date_column, date_format):

        '''
        Description: Converts a Date Column to a Standard Pandas Date Format.

        Aurguments:
        dataframe_in = Dataframe.
        date_column = Name of the Date Column.
        date_format = Format of a date Ex:  
        todays_date = tuple containing year, month, day in order
        '''
        dataframe = df_new.copy()
        dataframe[date_column] = pd.to_datetime(dataframe[date_column], format=date_format)
        dataframe[date_column+'_Year'] = dataframe[date_column].dt.year
        dataframe[date_column+'_Month'] = dataframe[date_column].dt.month
        dataframe[date_column+'_Week'] = dataframe[date_column].dt.week
        dataframe[date_column+'_Day'] = dataframe[date_column].dt.day
        dataframe[date_column+'_Dayofweek'] = dataframe[date_column].dt.dayofweek
        #today_date = pd.to_datetime(dt.date(todays_date[0],todays_date[1],todays_date[2]))
        #dataframe[date_column+'_Recency'] = dataframe[date_column].apply(lambda x : (today_date - x).days)
        return dataframe
    
    def All_Label_Encoder(self, how):
    
    
        '''
        Description : This is a Label Encoding Custom Function which makes changes into the dataframe directly.

        Arguments:
        dataframe = Dataframe to be encoded.
        how = A string which indicates the method of encoding , {'numerical', 'probability'}
        '''
        df = self.copied_data
        df = self.EDA_Data()
        
        new_data=self.handle_dates(df_new=df,date_column= 'FirstOrderedDate', date_format='%Y-%m-%d')
        df=self.handle_dates(df_new=new_data, date_column='LastOrderedDate',date_format= '%Y-%m-%d')
        df.drop(['FirstOrderedDate','LastOrderedDate'], axis=1, inplace=True)
        def Label_mapper(name,df,how):

            if how=='numerical':
                sam_dict={}
                num=0
                for loc in df[name].unique():
                    sam_dict.update([(loc,num)])
                    num=num+1
                df[name]=df[name].map(sam_dict)

            elif how=='probability':
                unique_names=[]
                for ind in df[name].value_counts().index:
                    unique_names.append(ind)
                unique_values=[]
                for val in df[name].value_counts():
                    unique_values.append(val)
                total_sum=sum(unique_values)
                new_unique_values = []
                for vals in unique_values:
                    new_unique_values.append(vals/total_sum)
                proba_dict = {unique_names[i]: new_unique_values[i] for i in range(len(unique_names))}
                df[name]=df[name].map(proba_dict)

        for columns in df.select_dtypes(include=['object']).columns:
            Label_mapper(columns,df,how)
        return df
    
    def SplittingData(self):
        df = self.All_Label_Encoder(how ='numerical')
    
        predictors = df.drop(['Final_Churn','accountsapid'],axis=1)
        target = df['Final_Churn']
        train_X, test_X, train_y, test_y = train_test_split(predictors, target, test_size=0.3, random_state=42)

        return predictors,target,train_X,test_X,train_y,test_y
    
    def Imp_feats_kbest(self,n_feats, function, X_train, Y):
        
        feat_selector = SelectKBest(function, k=n_feats)
        feat_selector.fit(X_train, Y)
        kbestcols = feat_selector.get_support(indices=True)
        imp_features = X_train.iloc[:,kbestcols].columns
        return imp_features
    
    def ModelCreation(self):      

        predictors,target,train_X,test_X,train_y,test_y = self.SplittingData()
        LR_kbest = self.Imp_feats_kbest(n_feats=21,function=f_classif,X_train=predictors,Y=target)
        final_Logistic_Model = LogisticRegression(C=0.030)
        final_Logistic_Model.fit(train_X[LR_kbest], train_y)

        print('train accuracy',recall_score(final_Logistic_Model.predict(train_X[LR_kbest]),train_y))
        print('test accuracy',recall_score(final_Logistic_Model.predict(test_X[LR_kbest]),test_y))
        
        LR_y_pred = final_Logistic_Model.predict_proba(predictors[LR_kbest])
        client_data = self.copied_data
        client_data= client_data[['accountsapid' , '2019' , '2020', '2021' , 'Churn_Rule_year_wise','Median_Days','3_years_cycle','Churn_Rule_avg_cycle', 'Final_Churn']]
        client_data['Final_churn_ProbabilityScore'] = LR_y_pred[:,1]
        return client_data


from azureml.core.run import Run
import os

rr= Run.get_context()
data = rr.input_datasets['raw_data'].to_pandas_dataframe()


Merged_data = DataCleaning(data).FinalData()
client_data = Model(Merged_data).ModelCreation()

mounted_output_dir = rr.output_datasets["my_output_data"]
os.makedirs(os.path.dirname(mounted_output_dir), exist_ok=True)
client_data.to_csv(mounted_output_dir+"/client_data.csv",index=False)
