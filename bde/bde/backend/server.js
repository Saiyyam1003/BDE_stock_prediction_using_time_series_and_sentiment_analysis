const express = require('express');
const cassandra = require('cassandra-driver');
const cors = require('cors');
const morgan = require('morgan');

const app = express();
const port = 3001;

// Middleware
app.use(cors());
app.use(express.json());
app.use(morgan('dev')); // Logging middleware

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
        const combinedData = stockResult.rows.map(row => {
            const date = row.timestamp.toISOString().split('T')[0];
            const newsForDate = newsResult.rows.find(newsRow => 
                newsRow.timestamp.toISOString().split('T')[0] === date
            );
        
            return {
                date: date,
                price: parseFloat(row.price),
                symbol: row.symbol,
                volume: parseInt(row.volume),
                prediction: parseFloat(row.prediction),
                overallSentiment: newsForDate ? parseFloat(newsForDate.overall_sentiment) : null // Add overall sentiment
            };
        }); 
        res.json({
            status: 'success',
            data: combinedData
        });

        // Transform stock data
        // const stockData = stockResult.rows.map(row => ({
        //     date: row.timestamp.toISOString().split('T')[0],
        //     price: parseFloat(row.price),
        //     symbol: row.symbol,
        //     volume: parseInt(row.volume),
        //     prediction: parseFloat(row.prediction)
        // }));

        // // Transform news data
        // const newsData = newsResult.rows.map(row => ({
        //     date: row.timestamp.toISOString().split('T')[0],
        //     company: row.company,
        //     title: row.title,
        //     description: row.description,
        //     sentiment: {
        //         title: parseFloat(row.title_sentiment),
        //         description: parseFloat(row.description_sentiment),
        //         overall: parseFloat(row.overall_sentiment),
        //         label: row.sentiment_label
        //     }
        // }));
        // console.log(stockData,newsData);

        // res.json({
        //     status: 'success',
        //     data: {
        //         stock: stockData,
        //         news: newsData
        //     }
        // });

    } catch (err) {
        console.error(`Error fetching data for ${symbol}:`, err);
        res.status(500).json({
            status: 'error',
            message: 'Internal server error',
            error: process.env.NODE_ENV === 'development' ? err.message : undefined
        });
    }
});

// API to get the list of companies
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

// Start the server
app.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});

module.exports = app; // For testing purposes