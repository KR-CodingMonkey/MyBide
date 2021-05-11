$("form[name=signup_form").submit(function(e) {
    // singup form 버튼이 눌리면 발생하는 이벤트
      var $form = $(this);
      var $error = $form.find(".error");
      var data = $form.serialize();
      console.log($form)
      console.log(data)      

      $.ajax({
        // url: "/user/signup",
        url: "http://localhost:15000/user/signup", // linux
        // url: "http://localhost:5000/user/signup", // windows/
        type: "POST",
        data: data,
        dataType: "json",
        success: function(data, textStatus, xhr){
          console.log("Success!");
          alert('Register Success');
          location.href = "main.html";
        },
        error: function(data, textStatus, xhr) {
          switch(data.status){
            case 300:
              alert('이미 존재하는 아이디입니다.');
              console.log("Error!");
            default:
              console.log(data.status);
          }
          location.reload();
        }
      });
    
      e.preventDefault();
    });
