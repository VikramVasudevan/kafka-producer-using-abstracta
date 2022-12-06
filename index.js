const axios = require('axios');
const qs = require('qs');
const { v4: uuidv4 } = require('uuid');

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

const pageSize = 50;
const numSamples = 1000000;
const bufferSize = 500;
async function getData(token, from, to, rowsFetchedSoFar) {
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
        response = await getData(token, from + pageSize, to + pageSize, rowsFetchedSoFar);
    }
    return response;
}

async function addData(token, data) {
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

async function authAndGetData() {
    const token = await authenticate();
    const response = await getData(token, 1, pageSize, 0);
    console.log("response = ", response?.data);

}

async function authAndPostData() {
    let token = await authenticate();
    var data = [];

    for (var i = 0; i < numSamples; i++) {
        const message = {
            "text": i + ". some random text - " + uuidv4()
        };
        if (data.length < bufferSize) {
            // console.log('Adding message ', i)
            data.push(message)
        }
        else {
            console.log(new Date(), i, "Ingesting next", data.length, 'records to Kafka through Abstracta');
            // console.log('Adding message ', i)
            try {
                await addData(token, data);
            } catch (e) {
                //Unauthorized
                // Retry again with a fresh token
                console.warn("!!!WARNING!!! - TOKEN EXPIRED ... REGENERATING TOKEN");
                token = await authenticate();
                await addData(token, data);
            }

            data = [message];
        }
        // console.log("response = ", response.data);
    }

}

async function addAndFetchData() {
    const startTime = new Date();
    await authAndPostData();
    // await authAndGetData();
    console.log("Started ", startTime);
    console.log(`Finished Adding ${numSamples} records to Kafka through Abstracta`, new Date());
}

addAndFetchData();