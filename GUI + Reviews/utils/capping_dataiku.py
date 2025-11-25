import pandas as pd
import numpy as np

DataFrame = pd.core.frame.DataFrame
Weights = np.ndarray[np.float_]
CappingFactors = np.ndarray[np.float_]

def proportional_capping(df: DataFrame, stock_max_weight: float, max_tries: int=100) -> (Weights, CappingFactors):
    """Computes capping factors redistributing the excess weight proportional to the intial weights
    Args:
        df (pd.DataFrame): weighted stocks
        stock_max_weight (float): capping threshold between 0 and 1 (eg 0.10 for 10%)
        max_tries (int): maximum number of iterations (to avoid infinite loop)

    Returns:
        np.ndarray[np.float_]: weights array 
        np.ndarray[np.float_]: capping factors array
    """
    df['Initial_weight'] = df['Weight']
    weight_col = 'Weight'

    iteration = 0
    while df[df[weight_col].round(14) > stock_max_weight].shape[0] > 0:
        if iteration > max_tries:
            raise Exception(f"Capping procedure reached the max number of iterations ({max_tries}).")
        iteration += 1
        
        to_cap_count = df[df[weight_col].round(14)>=stock_max_weight].shape[0] # count how many companies need capping
        final_weight_uncapped = 1.0 - (stock_max_weight * to_cap_count) # compute how much weight should be allocated to the non capped companies
        initial_weight_uncapped = df[df[weight_col].round(14)<stock_max_weight][weight_col].sum() # compute how much weight the non capped companies currently have
        weight_increase_ratio = (final_weight_uncapped / initial_weight_uncapped) - 1 
        df[f'Weight_after_{iteration}'] = np.where(df[weight_col] >= stock_max_weight,
                                                   stock_max_weight,
                                                   df[weight_col] * (1 + weight_increase_ratio)) 
        weight_col = f'Weight_after_{iteration}'

    df['Weight'] = df[weight_col]
    df['weight_change'] = df['Weight'] / df['Initial_weight']
    df['Capping'] = np.where(df['Weight'] < stock_max_weight,
                             1,
                             df['weight_change'] / df['weight_change'].max())

    return df.index.map(df['Weight']), df.index.map(df['Capping'])

def custom_capping_stock_industry(df: DataFrame, 
                                  stock_max_weight: float, 
                                  industry_max_weight: float,
                                  max_tries: int=100) -> (Weights, CappingFactors):

    initial_weight = df['Weight']
    print(f"initial weights: {initial_weight.tolist()}")

    iteration = 0
    while True:
        if iteration > max_tries:
            raise Exception(f"Capping procedure reached the max number of iterations ({max_tries}).")
        iteration += 1

        df['Weight'], _ = proportional_capping(df, stock_max_weight=stock_max_weight)
        print(f"#{iteration} normal capping")

        industry_weights = df.groupby('Industry Name', as_index=False)['Weight'].sum()
        industry_to_cap = industry_weights.loc[industry_weights['Weight']>industry_max_weight]

        if industry_to_cap.empty:
            break
        else:
            print(f"#{iteration} industry capping")
            weight_to_redistribute = industry_to_cap['Weight'].tolist()[0] - industry_max_weight
            df['Industry Weight'] = df.groupby('Industry Name')['Weight'].transform(sum)

            mask_to_cap = (df['Industry Name'].isin(industry_to_cap['Industry Name']))
            mask_to_receive = (~df['Industry Name'].isin(industry_to_cap['Industry Name'])) & (df['Weight'] < 0.10)

            df.loc[mask_to_cap, 'New Weight'] = df['Weight'] * (industry_max_weight/df['Industry Weight'])
            df.loc[mask_to_receive, 'New Weight'] = df['Weight'] + (weight_to_redistribute * 
                                                                    (df['Weight'] / df.loc[mask_to_receive, 'Weight'].sum()))
            df.loc[~mask_to_cap & ~mask_to_receive, 'New Weight'] = df['Weight']

            df.drop('Weight', axis=1, inplace=True)
            df.rename(columns={'New Weight': 'Weight'}, inplace=True)

    df['weight_change'] = df['Weight'] / initial_weight
    df['Capping'] = df['weight_change'] / df['weight_change'].max()

    return df.index.map(df['Weight']), df.index.map(df['Capping'])
 