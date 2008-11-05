
## Protocol extensions

The protocol can be extended in several ways, a few of which are illustrated
in the following example. We'll be extending this trivial protocol definition:

    (* this is a comment (* and this a nested comment *) *)
    message user = {
      id : int;
      name : string;
    }

### New message fields

Suppose we find out some time later we also need the email and the age:

    message user = {
      id : int;
      name : string;
      email : string;
      age : int
    }

Adding new fields this way means that older readers can consume data from new
producers. If we also want new consumers to read old data, they have to cope
with the possibility that the new fields be missing, using disjoint unions
(sum types) as explained below. For now, let's keep things simple.

### New tuple elements

Then we realize that all users have at least an email, but maybe more, so we
extend the message again:

    message user = {
      id : int;
      name : string;
      email : (string * [string]);  (* at least one email, maybe more *)
      age : int
    }

The email field is now a tuple with two elements, the first one being a
string, and the second one a list of strings that might be empty (in this
case, the user has got only one email).

### Disjoint unions (sum types)

Imagine our application has got several user types:

* free user
* paying user: we also want to record the end of the subscription period

This can be captured in the following type definition:

    type date = float (* time in seconds since the start of the epoch  *)

    type user_type = Free | Paying date
		            (* could be written as  Paying float *)

    message user = {
      id : int;
      name : string;
      emails : (string * [string]);  (* at least one email, maybe more *)
      age : int;
      user_type : user_type
    }

That's not all: we then decide that all users qualify for a discount rate one
time starting from now.

    (* whether we will offer a discount rate in the next renewal *)
    type discount = Yes | No

    type user_type = Free | Paying date discount

    (* same user definition as above *)

Old records of paying users have no discount element in their user_type field,
so the value will default to "Yes" when it is read by new consumers --- if we
wanted it to be "No" by default, we'd simply have to define the discount type
as

    type discount = No | Yes

### Polymorphic types

After a while, we have several message definitions, and realize that the "at
least one" pattern happens often. We can use a polymorphic type to avoid
having to type "(x * [x])" again and again:

    type one_or_more 'x = ('x * ['x])

    message user = {
      id : int;
      name : string;
      emails : one_or_more<string>;
      age : int;
      user_type : user_type;
    }

### Conbining polymorphism and sum types

Going back to the first extension we did, disjoint unions allow us to know when
a field is missing and to handle that case.

    type option 'a = None | Some 'a

    message user = {
      ...
      age : option<int>;
      ...
    }

This is no other than the option type from ML (Maybe in Haskell). A consumer
with the new type definition will know when a field is missing because the
value will be set to None. Refer to the documentation on the
[target language mappings](language-mapping.md) to see how this translates in
the target language.