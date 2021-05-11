alert('test');

$.ajax({
    type: "GET",
    enctype: 'multipart/form-data',
    // url: "http://localhost:15000/upload/complete", // linux
    url: "http://localhost:5001/user/loadimage", // windows
    // data: data,/
    processData: false,
    contentType: false,
    cache: false,
    dataType: "json",

    success: function(data, textStatus, xhr){
        alert('success')
        console.log(data)
        document.getElementById("image1").src = data[0]
        console.log(data[0])
        // location.href = "main.html";
    },
    
    error: function(data, textStatus, xhr) {
    switch(data.status){
        case 300:
        default:
        console.log('error');
        console.log(data.status);
        }
    }
});