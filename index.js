const axios = require('axios');
const qs = require('qs');
const { v4: uuidv4 } = require('uuid');

const PAGE_SIZE = 50;
const numSamples = 1000000;
const BUFFER_SIZE = 750;
const MAX_ABSTRACTA_THREADS = 25;

const isTokenExpired = (token) => (Date.now() >= JSON.parse(Buffer.from(token.split('.')[1], 'base64').toString()).exp * 1000)

let G_AUTH_TOKEN;

const apiCreds = {
    grant_type: "client_credentials",
    client_id: '7ff4dcfc-ce2e-4901-86e0-c9d7a0a6b355',
    client_secret: 'jvBudo2OBr2d63FcWzW0hsOiez9yLX8I',
    audience: "http://localhost:8080/"
}
async function authenticate() {
    console.log("Fetching auth token ...");
    var data = qs.stringify(apiCreds);
    var config = {
        method: 'post',
        url: 'http://localhost:8180/auth/realms/abstracta/protocol/openid-connect/token',
        headers: {
            'content-type': 'application/x-www-form-urlencoded'
        },
        data: data
    };

    const response = await axios(config)
    return response.data.access_token;
}

async function getData(from, to, rowsFetchedSoFar) {
    const token = await refreshToken();
    console.log("Fetching data...", arguments);
    var data = {
        "from": from,
        "to": to,
        "columns": "*",
        "pipes": "",
        "format": "application/json",
        "forUser": "vikram.vasudevan@ekahaa.com",
        "forUserSecret": "3003c292-26ee-48d0-84ec-de0ce8412fe9",
        "_isbrowsemode": "true",
        lean: true
    };

    var config = {
        method: 'post',
        url: 'http://localhost:8080/rest/data/queryv2/demo_001/app_001/kafka_002/api_001/0.0.0',
        headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json'
        },
        data: data
    };

    let response = await axios(config)
    rowsFetchedSoFar += response?.data?.length;
    console.log("rowsFetched = ", rowsFetchedSoFar)
    if (response?.data?.length > 0) {
        response = await getData(from + PAGE_SIZE, to + PAGE_SIZE, rowsFetchedSoFar);
    }
    return response;
}

async function addData(data) {
    const token = await refreshToken();
    var config = {
        method: 'post',
        url: 'http://localhost:8080/rest/data/post/demo_001/app_001/kafka_002/api_001/0.0.0',
        headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json'
        },
        data: data
    };

    const response = await axios(config)
    return response;
}

async function refreshToken() {
    if (!G_AUTH_TOKEN || isTokenExpired(G_AUTH_TOKEN)) {
        G_AUTH_TOKEN = await authenticate();
    }

    return G_AUTH_TOKEN;
}
async function authAndGetData() {
    const token = await refreshToken();
    const response = await getData(token, 1, PAGE_SIZE, 0);
    console.log("response = ", response?.data);

}

async function authAndPostData() {
    var data = [];
    var promises = [];

    for (var i = 0; i < numSamples; i++) {
        const message = {
            "text": i + ". some random text - " + uuidv4()
        };
        if (data.length < BUFFER_SIZE) {
            // console.log('Adding message ', i)
            data.push(message)
        }
        else {
            console.log(new Date(), i, "Ingesting next", data.length, 'records to Kafka through Abstracta');
            // console.log('Adding message ', i)
            if (promises.length <= MAX_ABSTRACTA_THREADS)
                promises.push(addDataWithTokenGen(data));
            else {
                await Promise.all(promises);
                promises = [addDataWithTokenGen(data)];
            }
            data = [message];
        }
        // console.log("response = ", response.data);
    }

}

async function addDataWithTokenGen(data) {
    try {
        await refreshToken();
        await addData(data);
    } catch (e) {
        //Unauthorized
        // Retry again with a fresh token
        console.warn("Error Posting Data", e);
    }
}

async function addAndFetchData() {
    const startTime = new Date();
    await refreshToken();
    await authAndPostData();
    // await authAndGetData();
    console.log("Started ", startTime);
    console.log(`Finished Adding ${numSamples} records to Kafka through Abstracta`, new Date());
}

addAndFetchData();