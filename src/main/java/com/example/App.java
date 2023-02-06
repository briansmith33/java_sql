package com.example;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import io.github.cdimascio.dotenv.Dotenv;


public class App 
{
    
    public static void main( String[] args )
    {
        String id = "984c598b-416b-4a8a-8902-34b8e35b6a4b";
        Dotenv dotenv = Dotenv.load();
        DBConn db = new DBConn(
            dotenv.get("DB_DRIVER"), 
            dotenv.get("DB_HOST"), 
            dotenv.get("DB_PORT"), 
            dotenv.get("DB_DBNAME"), 
            dotenv.get("DB_USER"), 
            dotenv.get("DB_PASS")
        );
        
    }

}
