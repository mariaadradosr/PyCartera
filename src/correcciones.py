
import pandas as pd

# def SegmentoCorrecciones(row):
#     if row['assetid'] == '02i0N00000PfMuWQAV':
#         return '4-MIGRADO'
#     elif row['assetid'] == '02i0N00000Pfg96QAB':
#         return '3-NOTRIAL'
#     else:
#         return row['segmento']

def DRD(row):
    return pd.NaT if row['assetid'] in ['02i0N00000KqDCzQAN','02i0N00000I2qZ2QAJ','02i0N00000I2qZ3QAJ'] else row['deactivation_request_date']

def DD(row):
    # return pd.NaT if row['assetid'] in ['02i0N00000L2rIBQAZ','02i0N00000L2rICQAZ','02i0N00000LGrVPQA1','02i0N00000LGrVQQA1'] else row['deactivation_date']
    if row['assetid'] in ['02i0N00000KqDCzQAN','02i0N00000L2rIBQAZ','02i0N00000L2rICQAZ','02i0N00000LGrVPQA1','02i0N00000LGrVQQA1','02i0N00000LJWycQAH','02i0N00000HfjEZQAZ','02i0N00000HfjEYQAZ']:
        return pd.NaT
    else:
        return row['deactivation_date']

def CD(row):
    return row['deactivation_date'] if row['assetid'] in ['02i0N00000KqDCzQAN','02i0N00000LJWycQAH','02i0N00000HfjEZQAZ','02i0N00000HfjEYQAZ'] else row['cancellation_date']


def partner(row):
    if row['cif'] in ['P1817800D','B71332548']:
        return 'DHO'
    if row['cif'] in ['B88280359']:
        return 'IT integrator'
    else:
        return row['canal_venta']