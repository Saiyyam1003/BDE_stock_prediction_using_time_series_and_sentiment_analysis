const express = require('express');
const cassandra = require('cassandra-driver');
const cors = require('cors');
const morgan = require('morgan');
const fs = require('fs').promises;
const path = require('path');

const app = express();
const port = 3001;

// Middleware
app.use(cors());
app.use(express.json());
app.use(morgan('dev'));



const DATA_FILE = 'market_data.json';

// Cassandra connection setup
const client = new cassandra.Client({
    contactPoints: ['127.0.0.1'],
    localDataCenter: 'datacenter1',
    keyspace: 'stock_analysis',
    pooling: {
        maxRequestsPerConnection: 32768,
        coreConnectionsPerHost: {
            [cassandra.types.distance.local]: 2,
            [cassandra.types.distance.remote]: 1
        }
    },
    queryOptions: {
        consistency: cassandra.types.consistencies.localQuorum
    }
});

// Connect to Cassandra
client.connect()
    .then(() => console.log('Connected to Cassandra'))
    .catch(err => {
        console.error('Error connecting to Cassandra:', err);
        process.exit(1);
    });

// Function to load coefficients
async function loadCoefficients() {
    try {
        const data = await fs.readFile(path.join(__dirname, 'coefficients.json'), 'utf8');
        return JSON.parse(data);
    } catch (error) {
        console.warn('Could not load coefficients, using defaults:', error.message);
        return { x: 0.5, y: 0.3, z: 0.2 }; // Default values
    }
}


async function loadOrCreateMarketData() {
    try {
        const data = await fs.readFile(DATA_FILE, 'utf8');
        return JSON.parse(data);
    } catch (error) {
        return null;
    }
}

// Function to save market data
async function saveMarketData(data) {
    await fs.writeFile(DATA_FILE, JSON.stringify(data, null, 2));
    console.log(`Market data saved to ${DATA_FILE}`);
}

// Calculate final prediction using the non-linear formula
function calculateFinalPrediction(prediction, sentiment, coeffs) {
    const { x, y, z } = coeffs;
    return x * prediction + y * sentiment + z * (prediction * sentiment);
}

// Queries
const FETCH_QUERY = `
    SELECT timestamp, price, symbol, volume, prediction 
    FROM stock_analysis.prediction 
    WHERE symbol = ? 
    ORDER BY timestamp ASC;
`;

const FETCH_QUERY_NEWS = `
    SELECT *
    FROM stock_analysis.news_sentiment 
    WHERE company = ? 
    ORDER BY timestamp ASC;
`;

// API to fetch stock data for a specific company symbol
app.get('/api/data/:symbol', async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();


    try {
        // Load coefficients
        

        // Fetch stock data and news data concurrently
        const [stockResult, newsResult] = await Promise.all([
            client.execute(FETCH_QUERY, [symbol], { prepare: true }),
            client.execute(FETCH_QUERY_NEWS, [symbol], { prepare: true })
        ]);

        if (stockResult.rows.length === 0) {
            return res.status(404).json({
                status: 'error',
                message: `No data found for symbol ${symbol}`
            });
        }

        const newsData = newsResult.rows.map(row => {
            const date = row.timestamp.toISOString().split('T')[0];
            return {
                date: date,
                company: row.company,
                sentiment: parseFloat(row.overall_sentiment) || 0, // Default to 0 if sentiment is null/undefined
            };
        });
        
        // Map sentiment by company (or company and date if needed)
        const sentimentMap = newsData.reduce((acc, item) => {
            acc[item.company] = item.sentiment; // Overwrite sentiment if multiple entries exist
            return acc;
        }, {});
        
        const combinedData = stockResult.rows.map(row => {
            const date = row.timestamp.toISOString().split('T')[0];
            const sentiment = sentimentMap[row.symbol] || 0; // Default to 0 if no sentiment found for the company
            
            const rawPrediction = parseFloat(row.prediction);
            const finalPrediction = rawPrediction * 1 + sentiment * 0.00; // Apply the formula

            return {
                date: date,
                price: parseFloat(row.price),
                symbol: row.symbol,
                volume: parseInt(row.volume),
                rawPrediction: parseFloat(row.prediction),
                sentiment: sentiment,
                finalPrediction : finalPrediction
            };
        });
        
        console.log("Combined Data:", combinedData);

        
        res.json({
            status: 'success',
            data: combinedData 
        });

    } catch (err) {
        console.error(`Error fetching data for ${symbol}:`, err);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: process.env.NODE_ENV === 'development' ? err.message : undefined
        });
    }
});

// Rest of the code remains the same...
app.get('/api/companies', (req, res) => {
    const companies = [
        { symbol: 'AAPL', name: 'Apple Inc.' },
        { symbol: 'MSFT', name: 'Microsoft Corporation' },
        { symbol: 'GOOGL', name: 'Alphabet Inc.' },
        { symbol: 'AMZN', name: 'Amazon.com Inc.' },
        { symbol: 'META', name: 'Meta Platforms Inc.' },
        { symbol: 'NVDA', name: 'NVIDIA Corporation' },
        { symbol: 'TSLA', name: 'Tesla Inc.' },
        { symbol: 'JPM', name: 'JPMorgan Chase & Co.' }
    ];
    res.json({
        status: 'success',
        data: companies
    });
});

// Error handling middleware
app.use((err, req, res, next) => {
    console.error(err.stack);
    res.status(500).json({
        status: 'error',
        message: 'Something broke!',
        error: process.env.NODE_ENV === 'development' ? err.message : undefined
    });
});

// Graceful shutdown
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);

async function shutdown() {
    console.log('Shutting down gracefully...');
    try {
        await client.shutdown();
        console.log('Cassandra client disconnected');
        process.exit(0);
    } catch (err) {
        console.error('Error during shutdown:', err);
        process.exit(1);
    }
}

app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});

module.exports = app;