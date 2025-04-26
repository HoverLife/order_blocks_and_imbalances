import pandas as pd

def load_data(filename):
    data = pd.read_csv(
        filename,
        sep=',',
        header=None,
        names=['datetime_str', 'open', 'high', 'low', 'close', 'volume'],
        dtype={'open': float, 'high': float, 'low': float, 'close': float, 'volume': int}
    )
    data['datetime'] = pd.to_datetime(data['datetime_str'], format='%Y%m%d %H%M%S')
    data.set_index('datetime', inplace=True)
    return data.drop(columns=['datetime_str'])

def find_order_blocks(df):
    #Поиск ордер-блоков на 1-часовом таймфрейме
    blocks = []
    for i in range(1, len(df)-2):
        try:
            prev = df.iloc[i-1]
            curr = df.iloc[i]
            next_1 = df.iloc[i+1]
            next_2 = df.iloc[i+2]
            
            # Бычий блок
            if (prev['close'] < prev['open'] and 
                curr['close'] > curr['open'] and 
                curr['close'] > (prev['open'] + prev['close'])/2 and 
                next_1['close'] < curr['close']):
                
                low = min(prev['low'], curr['low'])
                high = max(prev['high'], curr['high'])
                if next_2['high'] > high: high = next_2['high']
                blocks.append({'type': 'Бычий', 'datetime': curr.name, 'range': (low, high)})
            
            # Медвежий блок
            elif (prev['close'] > prev['open'] and 
                  curr['close'] < curr['open'] and 
                  curr['close'] < (prev['open'] + prev['close'])/2 and 
                  next_1['close'] > curr['close']):
                
                high = max(prev['high'], curr['high'])
                low = min(prev['low'], curr['low'])
                if next_2['low'] < low: low = next_2['low']
                blocks.append({'type': 'Медвежий', 'datetime': curr.name, 'range': (low, high)})
        
        except IndexError:
            break  # Если данные заканчиваются
    return pd.DataFrame(blocks)

def find_imbalances(df):
    #Поиск имбалансов на указанном таймфрейме.
    imbalances = []
    for i in range(1, len(df)-1):
        try:
            first = df.iloc[i-1]
            second = df.iloc[i]
            third = df.iloc[i+1]
            
            # Бычий имбаланс
            if (second['close'] > first['high'] and 
                second['close'] > third['high']):
                imb_range = (first['high'], third['low'])
                imbalances.append({'type': 'Бычий', 'datetime': second.name, 'range': imb_range})
            
            # Медвежий имбаланс
            elif (second['close'] < first['low'] and 
                  second['close'] < third['low']):
                imb_range = (first['low'], third['high'])
                imbalances.append({'type': 'Медвежий', 'datetime': second.name, 'range': imb_range})
        
        except IndexError:
            break
    return pd.DataFrame(imbalances)

def main():
    data = load_data('correct_NQ.csv')
    
    # Ресемплинг данных
    df_1h = data.resample('h').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    df_15m = data.resample('15min').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    # Поиск паттернов
    order_blocks = find_order_blocks(df_1h)
    imbalances_15m = find_imbalances(df_15m)
    
    # Формирование результатов
    results = []
    block_counter = 1
    
    #Добавляем все ордер-блоки
    for _, block in order_blocks.iterrows():
        block_entry = {
            'Параллельный номер': str(block_counter),
            'Формация': 'Ордер блок',
            'Направление': block['type'],
            'Дата и время': block['datetime'].strftime('%H:%M %d.%m.%Y'),
            'Диапазон цен': f"{block['range'][0]:.2f}$-{block['range'][1]:.2f}$"
        }
        results.append(block_entry)
        
        #Добавляем связанные имбалансы
        if not imbalances_15m.empty:
            matched_imbalances = imbalances_15m[
                (imbalances_15m['range'].apply(lambda x: block['range'][0] <= x[0])) &
                (imbalances_15m['range'].apply(lambda x: x[1] <= block['range'][1])) &
                (imbalances_15m['type'] == block['type'])
            ]
            
            for imb_num, (_, imb) in enumerate(matched_imbalances.iterrows(), 1):
                results.append({
                    'Параллельный номер': f"{block_counter}.{imb_num}",
                    'Формация': 'Имбаланс',
                    'Направление': imb['type'],
                    'Дата и время': imb['datetime'].strftime('%H:%M %d.%m.%Y'),
                    'Диапазон цен': f"{max(imb['range'][0], block['range'][0]):.2f}$-{min(imb['range'][1], block['range'][1]):.2f}$"
                })
        
        block_counter += 1
    
    #Добавляем имбалансы
    
    for imb_num, (_, imb) in enumerate(imbalances_15m.iterrows(), 1):
        results.append({
            'Параллельный номер': f"IB{imb_num}",
            'Формация': 'Имбаланс',
            'Направление': imb['type'],
            'Дата и время': imb['datetime'].strftime('%H:%M %d.%m.%Y'),
            'Диапазон цен': f"{imb['range'][0]:.2f}$-{imb['range'][1]:.2f}$"
        })
    
    
    # Экспорт результатов в новый файл
    pd.DataFrame(results).to_excel('results.xlsx', index=False)

if __name__ == '__main__':
    main()