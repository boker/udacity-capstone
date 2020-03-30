import numpy as np
import pandas as pd


def get_missing_dict(df):
    missing_dict = dict()
    missing_indicators = ['unknown','no transaction known','no transactions known']
    for i in df.index:
        if df['Meaning'][i] in missing_indicators:
            missing_dict[df['Attribute'][i]] = df['Value'][i]
    
    missing_dict['CAMEO_DEUINTL_2015'] = -1
    return missing_dict



def engineer_CAMEO_INTL_2015(df):
#create new ordinal attribute WEALTH_RATING
    df['WEALTH_RATING'] = df['CAMEO_INTL_2015'].str[:1].astype(float)

    #create new ordinal attribute FAMILY_TYPE
    df['FAMILY_TYPE'] = df['CAMEO_INTL_2015'].str[1:2].astype(float)

    df.drop(columns=['CAMEO_INTL_2015'], inplace=True)
    return df


def engineer_PRAEGENDE_JUGENDJAHRE(df):
        #create new binary attribute MOVEMENT with values Avantgarde (0) vs Mainstream (1)
    df['MOVEMENT'] = df['PRAEGENDE_JUGENDJAHRE']
    df['MOVEMENT'].replace([-1,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15], 
                           [np.nan,np.nan,1,2,1,2,1,2,2,1,2,1,2,1,2,1,2], inplace=True) 

    #create new ordinal attribute GENERATION_DECADE with values 40s, 50s 60s ... encoded as 4, 5, 6 ...
    df['GENERATION_DECADE'] = df['PRAEGENDE_JUGENDJAHRE']
    df['GENERATION_DECADE'].replace([-1,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15], 
                                    [np.nan,np.nan,4,4,5,5,6,6,6,7,7,8,8,8,8,9,9], inplace=True) 
    df.drop(columns=['PRAEGENDE_JUGENDJAHRE'], inplace=True)
    return df


def engineer_WOHNLAGE(df):
    df['RURAL_NEIGHBORHOOD'] = df['WOHNLAGE']
    df['RURAL_NEIGHBORHOOD'].replace([-1,0,1,2,3,4,5,7,8], [np.nan,np.nan,0,0,0,0,0,1,1], inplace=True)
    df.drop(columns=['WOHNLAGE'], inplace=True)
    return df


def engineer_PLZ8_BAUMAX(df):
    #create new binary attribute PLZ8_BAUMAX_BUSINESS with values Business (1) vs Not Business(0)
    df['PLZ8_BAUMAX_BUSINESS'] = df['PLZ8_BAUMAX']
    df['PLZ8_BAUMAX_BUSINESS'].replace([1,2,3,4,5], [0,0,0,0,1], inplace=True) 

    #create new ordinal attribute PLZ8_BAUMAX_FAMILY with from 1 to 4 encoded as in data dictionary
    df['PLZ8_BAUMAX_FAMILY'] = df['PLZ8_BAUMAX']
    df['PLZ8_BAUMAX_FAMILY'].replace([5], [0], inplace=True) 
    df.drop(columns=['PLZ8_BAUMAX'], inplace=True)
    return df



def mark_missing_values_as_null(df, missing_values_dict):
    for col in df.columns:
        if col in missing_values_dict.keys():
            unknown_values =  list(map(int, str(missing_values_dict[col]).strip().split(',')))
            for unknown_value in unknown_values:
                df[col] = df[col].replace(unknown_value, np.NaN)
            print(unknown_values)
    return df


def clean_data(df, delete_null_rows=False):
    missing_values_dict = get_missing_dict(df_values)
    df = mark_missing_values_as_null(df, missing_values_dict)

    cols = 'CAMEO_INTL_2015,CAMEO_DEUG_2015,CAMEO_DEU_2015'.split(',')
    for col in cols:
        df[col].replace('X', np.NaN, inplace=True)
        df[col].replace('XX', np.NaN, inplace=True)

    null_cols_to_drop = ['ALTER_KIND4', 'TITEL_KZ', 'ALTER_KIND3', 'D19_TELKO_ONLINE_DATUM',
                       'D19_BANKEN_LOKAL', 'D19_BANKEN_OFFLINE_DATUM', 'ALTER_KIND2',
                       'D19_TELKO_ANZ_12', 'D19_DIGIT_SERV', 'D19_BIO_OEKO',
                       'D19_TIERARTIKEL', 'D19_NAHRUNGSERGAENZUNG', 'D19_GARTEN',
                       'D19_LEBENSMITTEL', 'D19_WEIN_FEINKOST', 'D19_BANKEN_ANZ_12',
                       'D19_ENERGIE', 'D19_TELKO_ANZ_24', 'D19_BANKEN_REST',
                       'D19_VERSI_ANZ_12', 'D19_TELKO_OFFLINE_DATUM', 'D19_BILDUNG',
                       'ALTER_KIND1', 'D19_BEKLEIDUNG_GEH', 'D19_SAMMELARTIKEL',
                       'D19_BANKEN_ANZ_24', 'D19_FREIZEIT', 'D19_BANKEN_GROSS',
                       'D19_VERSI_ANZ_24', 'D19_SCHUHE', 'D19_HANDWERK', 'D19_TELKO_REST',
                       'D19_DROGERIEARTIKEL', 'D19_KINDERARTIKEL', 'D19_LOTTO',
                       'D19_KOSMETIK', 'D19_REISEN', 'D19_VERSAND_REST',
                       'D19_BANKEN_DIREKT', 'D19_BANKEN_ONLINE_DATUM', 'D19_TELKO_MOBILE',
                       'D19_HAUS_DEKO', 'D19_BEKLEIDUNG_REST', 'D19_BANKEN_DATUM',
                       'AGER_TYP', 'D19_TELKO_DATUM', 'D19_VERSICHERUNGEN', 'EXTSEL992',
                       'D19_VERSAND_ANZ_12', 'D19_VERSAND_OFFLINE_DATUM', 'D19_TECHNIK',
                       'D19_VOLLSORTIMENT', 'D19_GESAMT_ANZ_12', 'KK_KUNDENTYP',
                       'D19_VERSAND_ANZ_24', 'D19_GESAMT_OFFLINE_DATUM', 'D19_SONSTIGE',
                       'D19_GESAMT_ANZ_24', 'D19_VERSAND_ONLINE_DATUM', 'KBA05_BAUMAX',
                       'D19_GESAMT_ONLINE_DATUM', 'D19_VERSAND_DATUM', 'D19_GESAMT_DATUM']
    
    
    additional_columns_to_drop = ['CAMEO_DEU_2015', 'LNR']
    additional_columns_to_drop = "'LNR' 'ANZ_HH_TITEL' 'ANZ_KINDER' 'ANZ_TITEL' 'CAMEO_DEU_2015'" + \
                                " 'GEBAEUDETYP' 'GEBURTSJAHR' 'KBA05_MODTEMP' 'LP_FAMILIE_FEIN'" + \
                                " 'LP_LEBENSPHASE_FEIN' 'LP_LEBENSPHASE_GROB' 'LP_STATUS_FEIN' 'PLZ8_ANTG1'" + \
                                " 'PLZ8_ANTG2' 'PLZ8_ANTG3' 'PLZ8_ANTG4' 'VERDICHTUNGSRAUM' 'VK_DHT4A'" + \
                                " 'VK_DISTANZ' 'VK_ZG11' 'ALTERSKATEGORIE_FEIN' 'D19_TELKO_ONLINE_QUOTE_12'" + \
                                " 'D19_LETZTER_KAUF_BRANCHE' 'D19_VERSI_ONLINE_QUOTE_12'".replace("'","")
    additional_columns_to_drop = additional_columns_to_drop.split(' ')

    df = df.drop(columns = null_cols_to_drop)
    df.drop(columns=additional_columns_to_drop, inplace=True)
    
    # not sure what the column EINGEFUEGT_AM means, but it contains datetime value that should be meaningfully converted
    # either into numeric value... here the year of the date seems reasonable
    df["EINGEFUEGT_AM"] = pd.to_datetime(df["EINGEFUEGT_AM"], format='%Y/%m/%d %H:%M')
    df["EINGEFUEGT_AM"] = df["EINGEFUEGT_AM"].dt.year
    
    if delete_null_rows:
        df = df.dropna(thresh=len(df.columns)-30)
        df.reset_index(drop=True, inplace=True)

#       in case this is a customers dataset, then also drop the additional columns
    try:
        df = df.drop(columns=['PRODUCT_GROUP','CUSTOMER_GROUP','ONLINE_PURCHASE'])
    except:
        pass

    df =  engineer_PLZ8_BAUMAX( engineer_WOHNLAGE(engineer_PRAEGENDE_JUGENDJAHRE(engineer_CAMEO_INTL_2015(df))))
    df['OST_WEST_KZ'].replace(['O','W'], [0, 1], inplace=True)

#     df = pd.to_numeric(df) # just to be sure
    return df