
[![Tests](https://github.com/husio/masenko/workflows/Test/badge.svg)](https://github.com/husio/masenko/actions)
[![Docs](https://readthedocs.org/projects/masenko/badge/?version=latest&style=flat)](https://masenko.readthedocs.io/en/latest/)
![License](https://img.shields.io/badge/license-MIT-blue.svg)


![Logo](assets/masenko_logo.png)


Masenko is an open source background task manager with a focus on reliability
and complete, easy to use API.


See the [documentation](https://masenko.readthedocs.io/en/latest/) for more details.

[Unreliable benchmarks over time](http://benchsrv.herokuapp.com/).


## Go example


```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

mclient, err := masenkoclient.Dial("localhost:12345")
if err != nil {
	panic("cannot connect: " + err.Error())
}
defer mclient.Close()

// Task payload can be any JSON serializable data.
newUser := struct {
	Name  string
	Admin bool
}{
	Name:  "John Smith",
	Admin: false,
}

if err := mclient.Push(ctx, "register-user", "", newUser, "", 0, nil); err != nil {
	panic("cannot push task: " + err.Error())
}
```

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

mclient, err := masenkoclient.Dial("localhost:12345")
if err != nil {
	panic("cannot connect: " + err.Error())
}
defer mclient.Close()

for {
	response, err := mclient.Fetch(ctx, []string{"priority", "default"})
	if err != nil {
		panic("cannot fetch: " + err.Error())
	}

	switch response.Name {
	case "register-user":
		var newUser struct {
			Name  string
			Admin bool
		}
		if err := json.Unmarshal(response.Payload, &newUser); err != nil {
			panic("cannot unmarshal register-user task payload: " + err.Error())
		}
		err = handleRegisterUser(newUser)
	case "send-email":
		var email struct {
			Subject string
			To      string
			Content string
		}
		if err := json.Unmarshal(response.Payload, &email); err != nil {
			panic("cannot unmarshal send-email task payload: " + err.Error())
		}
		err = handleSendEmail(email)
	default:
		if err := mclient.Nack(ctx, response.ID); err != nil {
			panic("cannot NACK: " + err.Error())
		}
	}

	if err == nil {
		if err := mclient.Ack(ctx, response.ID); err != nil {
			panic("cannot ACK: " + err.Error())
		}
	} else {
		if err := mclient.Nack(ctx, response.ID); err != nil {
			panic("cannot NACK: " + err.Error())
		}
	}
}
```


## Python example

```python
with client.connect("localhost:12345") as masenko:
    masenko.push("register-user", {"name": "Jimmy", "weight": 74, "admin": False})
```

```python
handlers = {
    "register-user": regiter_user_handler,
    "send-email": send_email_handler,
}

with client.connect("localhost:12345") as masenko:
    while True:
        task = masenko.fetch(["high-prio", "default", "low-prio"])
        handler = handlers.get(task["name"])
        if not handler:
            # handle unknown task
        try:
            handler(task["payload"])
        except Exception:
            masenko.nack(task["id"])
        else:
            masenko.ack(task["id"])
```
