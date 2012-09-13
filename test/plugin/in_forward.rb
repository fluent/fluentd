require 'fluent/test'

class ForwardInputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  CONFIG = %[
    port 13998
    bind 127.0.0.1
  ]

  def create_driver(conf=CONFIG)
    Fluent::Test::InputTestDriver.new(Fluent::ForwardInput).configure(conf)
  end

  def test_configure
    d = create_driver
    assert_equal 13998, d.instance.port
    assert_equal '127.0.0.1', d.instance.bind
  end

  def connect
    TCPSocket.new('127.0.0.1', 13998)
  end

  def test_time
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    Fluent::Engine.now = time

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        send_data [tag, 0, record].to_msgpack
      }
      sleep 0.5
    end
  end

  def test_message
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        send_data [tag, time, record].to_msgpack
      }
      sleep 0.5
    end
  end

  def test_forward
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag1", time, {"a"=>2}

    d.run do
      entries = []
      d.expected_emits.each {|tag,time,record|
        entries << [time, record]
      }
      send_data ["tag1", entries].to_msgpack
      sleep 0.5
    end
  end

  def test_packed_forward
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag1", time, {"a"=>2}

    d.run do
      entries = ''
      d.expected_emits.each {|tag,time,record|
        [time, record].to_msgpack(entries)
      }
      send_data ["tag1", entries].to_msgpack
      sleep 0.5
    end
  end

  def test_packed_compressed_forward
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag1", time, {"a"=>2}

    d.run do
      entries = ''
      d.expected_emits.each {|tag,time,record|
        [time, record].to_msgpack(entries)
      }
      data = [
        "tag1",
        Zlib::Deflate.deflate(entries),
	{
          "detail" => { "compressed" => true, },
	},
      ]
      send_data data.to_msgpack
      sleep 0.5
    end
  end

  def test_message_json
    d = create_driver

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag2", time, {"a"=>2}

    d.run do
      d.expected_emits.each {|tag,time,record|
        send_data [tag, time, record].to_json
      }
      sleep 0.5
    end
  end

  CONFIG_ENCRYPTED = %[
    port 13998
    bind 127.0.0.1
    password pass
  ]
  def test_packed_encrypted_forward
    d = create_driver CONFIG_ENCRYPTED 

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag1", time, {"a"=>2}

    salt = OpenSSL::Random.random_bytes(8)

    d.run do
      entries = ''
      d.expected_emits.each {|tag,time,record|
        [time, record].to_msgpack(entries)
      }
      data = [
        encrypt("tag1", "pass", salt),
        encrypt(entries, "pass", salt),
	{
	  "encrypted" => true,
	  "salt" => salt,
          "detail" => encrypt({}.to_msgpack, "pass", salt),
	}
      ]
      send_data data.to_msgpack
      sleep 0.5
    end
  end

  def test_packed_encrypted_compressed_forward
    d = create_driver CONFIG_ENCRYPTED 

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i

    d.expect_emit "tag1", time, {"a"=>1}
    d.expect_emit "tag1", time, {"a"=>2}

    salt = OpenSSL::Random.random_bytes(8)

    d.run do
      entries = ''
      d.expected_emits.each {|tag,time,record|
        [time, record].to_msgpack(entries)
      }
      data = [
        encrypt("tag1", "pass", salt),
        encrypt(Zlib::Deflate.deflate(entries), "pass", salt),
        {
          "encrypted" => true,
          "salt" => salt,
          "detail" => encrypt({ "compressed" => true, }.to_msgpack, "pass", salt),
        },
      ]
      send_data data.to_msgpack
      sleep 0.5
    end
  end

  def encrypt(data, password, salt = '')
    cipher = OpenSSL::Cipher::BF.new
    key_iv = OpenSSL::PKCS5.pbkdf2_hmac_sha1(password, salt, 2000, cipher.key_len + cipher.iv_len)
    key = key_iv[0, cipher.key_len]
    iv = key_iv[cipher.key_len, cipher.iv_len]
    cipher.key = key
    cipher.iv = iv
    cipher.encrypt

    encrypted_data = ''
    encrypted_data << cipher.update(data)
    encrypted_data << cipher.final
    encrypted_data 
  end

  def send_data(data)
    io = connect
    begin
      io.write data
    ensure
      io.close
    end
  end

  # TODO heartbeat
end

