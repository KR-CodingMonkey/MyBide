$("#btnSubmit").click(function (event) {

    event.preventDefault(); 

    var form = $('#fileUploadForm')[0];
    var data = new FormData(form);
    console.log('data', data);
    
    $.ajax({
        type: "POST",
        enctype: 'multipart/form-data',
        url: "http://localhost:15000/upload/complete", // linux
        // url: "http://localhost:5000/upload/complete", // windows
        data: data,
        processData: false,
        contentType: false,
        cache: false,
        dataType: "json",

        success: function(data, textStatus, xhr){
            alert('success')
            location.href = "main.html";
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
});
    
function add_item(){
    // pre_set 에 있는 내용을 읽어와서 처리..
    var div = document.createElement('div');
    div.innerHTML = document.getElementById('keys').innerHTML;
    document.getElementById('field').appendChild(div);
}
function remove_item(obj){
    // obj.parentNode 를 이용하여 삭제
    document.getElementById('field').removeChild(obj.parentNode);
}