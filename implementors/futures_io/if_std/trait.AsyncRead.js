(function() {var implementors = {
"async_io":[["impl&lt;T:&nbsp;<a class=\"trait\" href=\"https://doc.rust-lang.org/1.66.1/std/io/trait.Read.html\" title=\"trait std::io::Read\">Read</a>&gt; <a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"async_io/struct.Async.html\" title=\"struct async_io::Async\">Async</a>&lt;T&gt;"],["impl&lt;T&gt; <a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a> for &amp;<a class=\"struct\" href=\"async_io/struct.Async.html\" title=\"struct async_io::Async\">Async</a>&lt;T&gt;<span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;for&lt;'a&gt; <a class=\"primitive\" href=\"https://doc.rust-lang.org/1.66.1/std/primitive.reference.html\">&amp;'a </a>T: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.66.1/std/io/trait.Read.html\" title=\"trait std::io::Read\">Read</a>,</span>"]],
"async_std":[["impl&lt;T:&nbsp;<a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">Read</a>, U:&nbsp;<a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">Read</a>&gt; <a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">AsyncRead</a> for <a class=\"struct\" href=\"async_std/io/struct.Chain.html\" title=\"struct async_std::io::Chain\">Chain</a>&lt;T, U&gt;"],["impl&lt;T:&nbsp;<a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">Read</a>&gt; <a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">AsyncRead</a> for <a class=\"struct\" href=\"async_std/io/struct.Take.html\" title=\"struct async_std::io::Take\">Take</a>&lt;T&gt;"],["impl&lt;R:&nbsp;<a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">Read</a>&gt; <a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">AsyncRead</a> for <a class=\"struct\" href=\"async_std/io/struct.BufReader.html\" title=\"struct async_std::io::BufReader\">BufReader</a>&lt;R&gt;"],["impl&lt;T&gt; <a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">AsyncRead</a> for <a class=\"struct\" href=\"async_std/io/struct.Cursor.html\" title=\"struct async_std::io::Cursor\">Cursor</a>&lt;T&gt;<span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.66.1/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;[<a class=\"primitive\" href=\"https://doc.rust-lang.org/1.66.1/std/primitive.u8.html\">u8</a>]&gt; + <a class=\"trait\" href=\"https://doc.rust-lang.org/1.66.1/core/marker/trait.Unpin.html\" title=\"trait core::marker::Unpin\">Unpin</a>,</span>"],["impl <a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">AsyncRead</a> for <a class=\"struct\" href=\"async_std/io/struct.Empty.html\" title=\"struct async_std::io::Empty\">Empty</a>"],["impl <a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">AsyncRead</a> for <a class=\"struct\" href=\"async_std/io/struct.Repeat.html\" title=\"struct async_std::io::Repeat\">Repeat</a>"],["impl <a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">AsyncRead</a> for <a class=\"struct\" href=\"async_std/io/struct.Stdin.html\" title=\"struct async_std::io::Stdin\">Stdin</a>"],["impl <a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">AsyncRead</a> for <a class=\"struct\" href=\"async_std/os/unix/net/struct.UnixStream.html\" title=\"struct async_std::os::unix::net::UnixStream\">UnixStream</a>"],["impl <a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">AsyncRead</a> for &amp;<a class=\"struct\" href=\"async_std/os/unix/net/struct.UnixStream.html\" title=\"struct async_std::os::unix::net::UnixStream\">UnixStream</a>"],["impl <a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">AsyncRead</a> for <a class=\"struct\" href=\"async_std/fs/struct.File.html\" title=\"struct async_std::fs::File\">File</a>"],["impl <a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">AsyncRead</a> for &amp;<a class=\"struct\" href=\"async_std/fs/struct.File.html\" title=\"struct async_std::fs::File\">File</a>"],["impl <a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">AsyncRead</a> for <a class=\"struct\" href=\"async_std/net/struct.TcpStream.html\" title=\"struct async_std::net::TcpStream\">TcpStream</a>"],["impl <a class=\"trait\" href=\"async_std/io/trait.Read.html\" title=\"trait async_std::io::Read\">AsyncRead</a> for &amp;<a class=\"struct\" href=\"async_std/net/struct.TcpStream.html\" title=\"struct async_std::net::TcpStream\">TcpStream</a>"]],
"blocking":[["impl&lt;T:&nbsp;<a class=\"trait\" href=\"https://doc.rust-lang.org/1.66.1/std/io/trait.Read.html\" title=\"trait std::io::Read\">Read</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/1.66.1/core/marker/trait.Send.html\" title=\"trait core::marker::Send\">Send</a> + 'static&gt; <a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"blocking/struct.Unblock.html\" title=\"struct blocking::Unblock\">Unblock</a>&lt;T&gt;"]],
"futures":[],
"futures_io":[],
"futures_lite":[["impl&lt;T:&nbsp;<a class=\"trait\" href=\"https://doc.rust-lang.org/1.66.1/std/io/trait.Read.html\" title=\"trait std::io::Read\">Read</a>&gt; <a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_lite/io/struct.AssertAsync.html\" title=\"struct futures_lite::io::AssertAsync\">AssertAsync</a>&lt;T&gt;"],["impl&lt;R:&nbsp;<a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a>&gt; <a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_lite/io/struct.BufReader.html\" title=\"struct futures_lite::io::BufReader\">BufReader</a>&lt;R&gt;"],["impl&lt;T&gt; <a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_lite/io/struct.Cursor.html\" title=\"struct futures_lite::io::Cursor\">Cursor</a>&lt;T&gt;<span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.66.1/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;[<a class=\"primitive\" href=\"https://doc.rust-lang.org/1.66.1/std/primitive.u8.html\">u8</a>]&gt; + <a class=\"trait\" href=\"https://doc.rust-lang.org/1.66.1/core/marker/trait.Unpin.html\" title=\"trait core::marker::Unpin\">Unpin</a>,</span>"],["impl <a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_lite/io/struct.Empty.html\" title=\"struct futures_lite::io::Empty\">Empty</a>"],["impl <a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_lite/io/struct.Repeat.html\" title=\"struct futures_lite::io::Repeat\">Repeat</a>"],["impl&lt;R:&nbsp;<a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a>&gt; <a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_lite/io/struct.Take.html\" title=\"struct futures_lite::io::Take\">Take</a>&lt;R&gt;"],["impl&lt;R:&nbsp;<a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a>&gt; <a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_lite/io/struct.Bytes.html\" title=\"struct futures_lite::io::Bytes\">Bytes</a>&lt;R&gt;"],["impl&lt;R1:&nbsp;<a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a>, R2:&nbsp;<a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a>&gt; <a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_lite/io/struct.Chain.html\" title=\"struct futures_lite::io::Chain\">Chain</a>&lt;R1, R2&gt;"],["impl&lt;T:&nbsp;<a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a> + <a class=\"trait\" href=\"https://doc.rust-lang.org/1.66.1/core/marker/trait.Unpin.html\" title=\"trait core::marker::Unpin\">Unpin</a>&gt; <a class=\"trait\" href=\"futures_io/if_std/trait.AsyncRead.html\" title=\"trait futures_io::if_std::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_lite/io/struct.ReadHalf.html\" title=\"struct futures_lite::io::ReadHalf\">ReadHalf</a>&lt;T&gt;"]],
"futures_util":[["impl&lt;A, B&gt; <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a> for <a class=\"enum\" href=\"futures_util/future/enum.Either.html\" title=\"enum futures_util::future::Either\">Either</a>&lt;A, B&gt;<span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;A: <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a>,<br>&nbsp;&nbsp;&nbsp;&nbsp;B: <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a>,</span>"],["impl&lt;St&gt; <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_util/stream/struct.IntoAsyncRead.html\" title=\"struct futures_util::stream::IntoAsyncRead\">IntoAsyncRead</a>&lt;St&gt;<span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;St: <a class=\"trait\" href=\"futures_util/stream/trait.TryStream.html\" title=\"trait futures_util::stream::TryStream\">TryStream</a>&lt;Error = <a class=\"struct\" href=\"https://doc.rust-lang.org/1.66.1/std/io/error/struct.Error.html\" title=\"struct std::io::error::Error\">Error</a>&gt;,<br>&nbsp;&nbsp;&nbsp;&nbsp;St::<a class=\"associatedtype\" href=\"futures_util/stream/trait.TryStream.html#associatedtype.Ok\" title=\"type futures_util::stream::TryStream::Ok\">Ok</a>: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.66.1/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;[<a class=\"primitive\" href=\"https://doc.rust-lang.org/1.66.1/std/primitive.u8.html\">u8</a>]&gt;,</span>"],["impl&lt;T&gt; <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_util/io/struct.AllowStdIo.html\" title=\"struct futures_util::io::AllowStdIo\">AllowStdIo</a>&lt;T&gt;<span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: <a class=\"trait\" href=\"https://doc.rust-lang.org/1.66.1/std/io/trait.Read.html\" title=\"trait std::io::Read\">Read</a>,</span>"],["impl&lt;R:&nbsp;<a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a>&gt; <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_util/io/struct.BufReader.html\" title=\"struct futures_util::io::BufReader\">BufReader</a>&lt;R&gt;"],["impl&lt;W:&nbsp;<a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a>&gt; <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_util/io/struct.BufWriter.html\" title=\"struct futures_util::io::BufWriter\">BufWriter</a>&lt;W&gt;"],["impl&lt;T, U&gt; <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_util/io/struct.Chain.html\" title=\"struct futures_util::io::Chain\">Chain</a>&lt;T, U&gt;<span class=\"where fmt-newline\">where<br>&nbsp;&nbsp;&nbsp;&nbsp;T: <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a>,<br>&nbsp;&nbsp;&nbsp;&nbsp;U: <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a>,</span>"],["impl&lt;T:&nbsp;<a class=\"trait\" href=\"https://doc.rust-lang.org/1.66.1/core/convert/trait.AsRef.html\" title=\"trait core::convert::AsRef\">AsRef</a>&lt;[<a class=\"primitive\" href=\"https://doc.rust-lang.org/1.66.1/std/primitive.u8.html\">u8</a>]&gt; + <a class=\"trait\" href=\"https://doc.rust-lang.org/1.66.1/core/marker/trait.Unpin.html\" title=\"trait core::marker::Unpin\">Unpin</a>&gt; <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_util/io/struct.Cursor.html\" title=\"struct futures_util::io::Cursor\">Cursor</a>&lt;T&gt;"],["impl <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_util/io/struct.Empty.html\" title=\"struct futures_util::io::Empty\">Empty</a>"],["impl <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_util/io/struct.Repeat.html\" title=\"struct futures_util::io::Repeat\">Repeat</a>"],["impl&lt;R:&nbsp;<a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a>&gt; <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_util/io/struct.ReadHalf.html\" title=\"struct futures_util::io::ReadHalf\">ReadHalf</a>&lt;R&gt;"],["impl&lt;R:&nbsp;<a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a>&gt; <a class=\"trait\" href=\"futures_util/io/trait.AsyncRead.html\" title=\"trait futures_util::io::AsyncRead\">AsyncRead</a> for <a class=\"struct\" href=\"futures_util/io/struct.Take.html\" title=\"struct futures_util::io::Take\">Take</a>&lt;R&gt;"]]
};if (window.register_implementors) {window.register_implementors(implementors);} else {window.pending_implementors = implementors;}})()