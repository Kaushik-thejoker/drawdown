# drawdown_logic.py
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
import logging
import io
import numpy as np
from typing import Dict

from drawdown_logic import calculate_drawdown

app = FastAPI()

# In-memory storage for processed data
processed_data: Dict[str, pd.DataFrame] = {}

# POST endpoint to upload CSV and calculate drawdown
@app.post("/nifty/upload_csv")
async def upload_csv(file: UploadFile = File(...), asset_type: str = Query(..., description="Select either 'nifty' or 'multi_asset'")):
    try:
        # Read the uploaded CSV file into a pandas DataFrame
        contents = await file.read()
        data = pd.read_csv(io.StringIO(contents.decode('utf-8')))
        
        # Select relevant columns based on asset type
        if asset_type == 'nifty':
            data = data[['Date', 'Nifty_price']].rename(columns={'Date': 'date', 'Nifty_price': 'price'})
        elif asset_type == 'multi_asset':
            data = data[['Date', 'Price']].rename(columns={'Date': 'date', 'Price': 'price'})
        else:
            raise HTTPException(status_code=400, detail="Invalid asset type. Please select either 'nifty' or 'multi_asset'.")
        
        # Drop any rows with missing values in 'date' or 'price'
        data = data.dropna(subset=['date', 'price'])
        
        # Convert 'price' column to numeric
        data['price'] = pd.to_numeric(data['price'], errors='coerce')
        
        # Drop rows with NaN values after conversion
        data = data.dropna(subset=['price'])
        
        # Convert 'date' column to datetime
        data['date'] = pd.to_datetime(data['date'])
        
        # Perform calculations on the DataFrame
        result = calculate_drawdown(data)
        
        # Ensure no NaN or infinite values are present in the result
        result_df = pd.DataFrame(result)
        result_df.replace([np.inf, -np.inf], np.nan, inplace=True)
        
        # Drop rows with NaN values
        result_df.dropna(inplace=True)
        
        # Convert 'date' column to string for JSON serialization
        result_df['date'] = result_df['date'].dt.strftime('%Y-%m-%d')
        
        # Store the result in memory
        processed_data[asset_type] = result_df
        
        return {"status": "success", "data": result_df.to_dict(orient='records'), "message": "Drawdown calculated successfully"}
    except ValueError as ve:
        logging.error(f"ValueError: {str(ve)}")
        raise HTTPException(status_code=500, detail="Invalid data encountered")
    except Exception as e:
        logging.error(f"An error occurred while processing the CSV: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to process CSV file")

# GET endpoint to retrieve processed data
@app.get("/nifty/data")
async def get_nifty_data(asset_type: str = Query(..., description="Select either 'nifty' or 'multi_asset'")):
    try:
        # Retrieve processed data from memory
        if asset_type not in processed_data:
            return {"status": "success", "data": [], "message": "No records found for the specified asset type"}
        
        data = processed_data[asset_type]
        # Convert 'date' column to string for JSON serialization
        data['date'] = data['date'].dt.strftime('%Y-%m-%d')
        
        return {"status": "success", "data": data.to_dict(orient='records')}
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
