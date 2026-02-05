# RailBreaker Library

Thie is a rust libary to support my open-source [RailBreaker Program](https://github.com/mdg1019/railbreaker).

I'm also using this code in an analysis program that I use to work on more analyzers. So having the common code in a single library, saves me time. 

By default, this code should reside in the same directory as your **railbreaker** source code folder.

So if **railbreaker** is in **Projects/railbreaker**, **railbreaker-lib** should be in **Projects/railbreaker-lib**.

You can change the location of **railbreaker-lib** by modifying the following in the **railbreaker** **cargo.toml** file to a different location:

```toml
[dependencies]
railbreaker-lib = { path = "../railbreaker-lib" }
```

When you've updated the source code for either project, be sure to do a ```git pull``` for both projects to be sure everything is in sync.
