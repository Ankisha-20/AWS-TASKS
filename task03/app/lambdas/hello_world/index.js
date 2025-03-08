exports.handler = async (event) => {
    const response = {
        statusCode: 200,
        message: JSON.stringify({"Hello from Lambda" }),
    };
    return response;
};
