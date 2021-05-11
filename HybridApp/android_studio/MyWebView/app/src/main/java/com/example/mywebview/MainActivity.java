package com.example.mywebview;

import android.annotation.SuppressLint;
import android.os.Bundle;
import android.view.KeyEvent;
import android.webkit.WebChromeClient;
import android.webkit.WebView;
import android.webkit.WebViewClient;

import androidx.appcompat.app.AppCompatActivity;

public class MainActivity extends AppCompatActivity {

    private WebView webView;
//    private String url = "https://www.naver.com";
    private String url = "file:///android_asset/login.html";

    @SuppressLint("WrongViewCast")
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        // 앱이 실행되는 실행주기
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        webView = (WebView)findViewById(R.id.webView); // activity xml의 id 값을 불러옴
        webView.getSettings().setJavaScriptEnabled(true); // javascript라는 언어를 허용해준다
        webView.loadUrl(url); // 특정 URL을 열어라
        webView.setWebChromeClient(new WebChromeClient()); // 웹뷰 환경을 구글 크롬에 맞춰서 최적화를 위한 추가 세팅
        webView.setWebViewClient(new WebViewClientClass()); // ''

    }
    // 뒤로가기 원래 화면으로 돌아가기 위한 세팅

    @Override
    public boolean onKeyDown(int keyCode, KeyEvent event) {
        // 뒤로가기 키를 입력했을때
        if ((keyCode == KeyEvent.KEYCODE_BACK) && webView.canGoBack()){
            webView.goBack();
            return true;
        }

        return super.onKeyDown(keyCode, event);
    }

    // 현재 페이지의 URL을 읽어올 수 있는 메소드, 새창을 읽어올 수도 있고, 특정페이지에서 특수한 기능을 넣을 수 있음.
    private class WebViewClientClass extends WebViewClient {
        @Override
        public boolean shouldOverrideUrlLoading(WebView view, String url) {
            view.loadUrl(url);
            return true;
        }
    }
}