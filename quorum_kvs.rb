require 'rubygems'
require 'bud'
require 'kvs/quorum_kvsproto'
require 'kvs/kvs'
require 'membership/membership'
# require '/home/tim/Desktop/CS194_Cloud/ptcrepo/lib/membership/membership'
# require '/home/tim/Desktop/CS194_Cloud/ptcrepo/lib/kvs/kvs'
# require '/home/tim/Desktop/CS194_Cloud/ptcrepo/lib/kvs/quorum_kvsproto'

require 'test/unit'

# Do a read before every write
# The value stored is a tuple containing a clock

module QuorumKVS
  include StaticMembership
  include QuorumKVSProtocol
  import BasicKVS => :kvs

  state do
    table :qconfig, quorum_config.schema

    scratch :num_r, [] => [:num]
    scratch :num_w, [] => [:num]

    channel :kvput_chan, [:@dest, :from] + kvput.key_cols => kvput.val_cols
    channel :kvget_chan, [:@dest, :from] + kvget.key_cols => kvget.val_cols
    channel :kvget_response_chan, [:@dest, :from] + kvget_response.key_cols => kvget_response.val_cols
    channel :kv_acks_chan, [:@dest, :from, :reqid]

    
    scratch :kvget_response_filter_by_clock_REAL, [:reqid, :key, :value, :clock_time]
    scratch :kvget_response_filter_by_clock, [:reqid, :key, :value, :clock_time] # This is a scratch that filters the responses so only the correct one is sent out
    
    table :kvresponse_buf, kvget_response_chan.schema
    table :kv_acks_buf, kv_acks_chan.schema
    table :need_resp, [:reqid]
    table :need_get_resp, [:reqid]
    table :need_ver_resp, [:reqid]
    table :new_put_value_store, [:reqid, :key] => [:value]

    scratch :send_acks, [:reqid]
    scratch :send_response, [:reqid]
    scratch :send_response_REAL, [:reqid]
    scratch :t_kvget_response, kvget_response.schema

    scratch :r_resp, kvget_response.schema
    scratch :put_debug, kvput_chan.schema
    scratch :acks_debug, kv_acks_chan.schema

    scratch :kvput_version_control_out, [:client, :key, :reqid] => [:value]
    scratch :filter_scratch, [:key, :reqid] => [:value, :clock_time]
    scratch :scratch_filler, [:client, :key] => [:reqid, :value]
    table :log_of_puts, [:key] # lets us know if there is anything to read.
    table :client_tracker, [:key, :reqid] => [:client]

  end

  bloom :q_config do
    qconfig <+ quorum_config do |t|
      [t.r_fraction, t.w_fraction] if qconfig.empty? and
        t.r_fraction >= 0 and t.r_fraction <= 1 and
        t.w_fraction >= 0 and t.w_fraction <= 1
    end
  end

  bloom :q_calc_r_w_num do
    num_r <= qconfig do |t|
      [t.r_fraction == 0 ? 1 : (t.r_fraction * member.length).ceil]
    end
    num_w <= qconfig do |t|
      [t.w_fraction == 0 ? 1 : (t.w_fraction * member.length).ceil]
    end
  end

  # requests are re-routed to member destination(s)
  bloom :requests do

    # If the key has never been stored then all the READs will fail, and thus nothing will ever output,
    # so forward the request immediatley.
    scratch_filler <= kvput.notin(log_of_puts, :key => :key) do |t, c| # note the body is not needed to work
    end

    kvput_version_control_out <= scratch_filler do |t|
      [t.client, t.key, t.reqid, [t.value, 0] ] # THIS IS US INITIALIZING THE CLOCK
    end
    log_of_puts <= kvput { |t| [t.key] }

    # Table to save the client as it gets lost in translation otherwise
    client_tracker <= kvput {|k| [k.key, k.reqid, k.client]}

    # Table letting us know what sent requests we need a response from
    need_resp <= kvput {|k| [[k.reqid]]}
    need_ver_resp <= kvput {|k| [[k.reqid]]}

    new_put_value_store <= kvput {|k| [k.reqid, k.key, k.value] }

    # Send a READ request to each node.
    # Prep by putting a READ request for each node into a socket/channel.
    kvget_chan <~ (member * kvput).pairs{|m,k| [m.host, ip_port, k.reqid, k.key]}

    # Recieve the READ request destined to 'this' node.
    kvs.kvget <= kvget_chan{ |k| k.to_a.drop(2) }

    # Put 'this' node's READ response onto a socket/channel destined to the client
    kvget_response_chan <~ (kvget_chan * kvs.kvget_response).outer(:reqid => :reqid) do |c, r|
      [c.from, ip_port] + r.to_a
    end

    # Add to a buffer ALL the nodes READ responses
    kvresponse_buf <= kvget_response_chan


    # send_response2 contains only a reqid, and only contains this when the correct number of
    # Read responses have returned. This allows us to identify when we can return the value of the read.
    send_response <= (kvresponse_buf * need_ver_resp).lefts.group([:reqid], count(:reqid)) do |t|
      if t.to_a.last >= num_r.max.first
        [t.first]
      end
    end

    need_ver_resp <- send_response
    kvresponse_buf <- (kvresponse_buf * send_response).lefts(:reqid => :reqid)
    
    # REAL KV GET VERSION
    send_response_REAL <= (kvresponse_buf * need_get_resp).lefts.group([:reqid], count(:reqid)) do |t|
      if t.to_a.last >= num_r.max.first
        [t.first]
      end
    end

    need_get_resp <- send_response_REAL
    
    kvget_response_filter_by_clock_REAL <= (kvresponse_buf * send_response_REAL).combos do |kvbuf, sr|
      if kvbuf.value != nil and kvbuf.reqid == sr.reqid
        [kvbuf.reqid, kvbuf.key, kvbuf.value, kvbuf.value[1]]
      end
    end

    # When send_response2 contains a reqid, the kvresponse_buf2 is looked at.
    # That is, when enough READ responses are available, we look at them,
    # and put all the responses in a table such that we can easily look at their
    # lamport clock values. (as we will need to find the largest one to identify the correct value)
    # [:reqid] => [:key, :value]
    kvget_response_filter_by_clock <= (kvresponse_buf * send_response * new_put_value_store).combos do |kvbuf, sr, nps|
      if kvbuf.value != nil and kvbuf.reqid == sr.reqid and sr.reqid == nps.reqid
        [kvbuf.reqid, kvbuf.key, nps.value, kvbuf.value[1]] if kvbuf.reqid == sr.reqid
      end
    end

    # Then the kv_responses are filtered so that those corresponding to the same value, are
    # selected with the highest clock time.
    # Basically if all the responses don't return the same value, then the value with the
    # latest clock time is chosen.
    # This clock time is then incremented by 1.
    # Now we have a correctly formated request.
    filter_scratch <= kvget_response_filter_by_clock.argmax([:reqid, :key, :value, :clock_time], :clock_time) do |t|
      [t.key, t.reqid, t.value, t.clock_time] # This value plus one is what the original WRITE request should send out
    end

    kvput_version_control_out <= (filter_scratch * client_tracker).pairs( :reqid => :reqid ) do |t, c|
      [c.client, t.key, t.reqid, [t.value, t.clock_time+1] ] # THIS IS US INCREMENTING THE CLOCK
    end

    kvput_chan <~ (member * kvput_version_control_out).pairs do |m,k|
      if k
        [m.host, ip_port, k.client, k.key, k.reqid, k.value]
      end
    end

 ##########****************KV-GET********************######################
    kvget_chan <~ (member * kvget).pairs{|m,k| [m.host, ip_port] + k.to_a}
    need_get_resp <= kvget{|k| [k.reqid] }
    
    t_kvget_response <= kvget_response_filter_by_clock_REAL.argmax([:reqid, :key, :value], :clock_time) do |t|
      [t.reqid, t.key, t.value[0]] # Here the clock_time is no longer attached to the value
    end
    kvget_response <= (t_kvget_response * need_get_resp).lefts(:reqid => :reqid)
    
    
  end

  # Receiver-side logic for re-routed requests
  bloom :receive_requests do

    kvs.kvput <= kvput_chan do |k|
      arr = k.to_a.drop(2)
    end

    kv_acks_chan <~ kvput_chan{|k| [k.from, ip_port, k.reqid] }
    acks_debug <= kvput_chan{|k| [k.from, ip_port, k.reqid] }

  end

  # forward responses to the original requestor node
  bloom :responses do
    # Add to buffer only those waiting for response
    kv_acks_buf <= kv_acks_chan

    # Once the required amount of ACKS have been recieved, send an ACK indicating
    # the write request succeeded on at least the minimum number of nodes
    send_acks <= (kv_acks_buf * need_resp).lefts.group([:reqid], count(:reqid)) do |t|
      [t.first] if t.to_a.last >= num_w.max.first
    end

    # Then the kv_responses are filtered so that those corresponding to the same value, are
    # selected with the highest clock time.
    # Basically if all the responses don't return the same value, then the value with the
    # latest clock time is chosen.


    need_resp <- send_acks
    need_get_resp <- kvget_response {|t| [t.reqid] }
    kv_acks <= send_acks

    # delete from buffer those that are not waiting for response
    kv_acks_buf <- (kv_acks_buf * kv_acks).lefts

    # *************************DEBUG*****************#
    # stdio <~ [["KVPUT: #{kvput.inspected}"]]
    stdio <~ [["NEED GET RESP: #{need_get_resp.inspected}"]]
    # stdio <~ [["KV RESPONSE BUF: #{kvresponse_buf.inspected}"]]
    # stdio <~ [["FILTER SCRATCH: #{filter_scratch.inspected}"]]
    stdio <~ [["kvput_version_control_out: #{kvput_version_control_out.inspected}"]]
    # stdio <~ [["CLIENT TRACKER: #{client_tracker.inspected}"]]
    # stdio <~ [["Send_acks: #{send_acks.inspected}"]]
     stdio <~ [["kvget_response_filter_by_clock: #{kvget_response_filter_by_clock.inspected}"]]
    # stdio <~ [["kvresponse_buf #{kvresponse_buf.inspected}"]]
    stdio <~ [["KV GET RESPONSE #{kvget_response.inspected}"]]
    # stdio <~ [["Send_response: #{send_response.inspected}"]]
    # stdio <~ [["kvs.kvput: #{kvs.kvput.inspected}"]]
    # stdio <~ [["KV STATE: #{kvs.kvstate.inspected}"]]
    # stdio <~ [["kv_acks_chan rx: #{kv_acks_chan.inspected}"]]
    # stdio <~ [["WTF?: #{kv_acks_buf.inspected} #{kvresponse_buf.inspected}" +
    #           "#{need_resp.inspected}"]]
    #stdio <~ [["q_calc, member.len: #{member.length}, num_r: #{num_r.inspected}, num_w: #{num_w.inspected}"]]
    # stdio <~ [["Acks debug: #{acks_debug.inspected}"]]

  end
end

class Q
  include Bud
  include QuorumKVS
end

class TestQ < Test::Unit::TestCase
  def tick
    @a.tick
    @b.tick
    @c.tick
    @d.tick
  end

  def init
    @a = Q.new(:port => 12300)
    @b = Q.new(:port => 12301)
    @c = Q.new(:port => 12302)
    @d = Q.new(:port => 12303)
  end

  def test_q
    init
    @a.sync_do{@a.add_member <+ [[0, @a.ip_port]]}
    @a.sync_do{@a.add_member <+ [[1, @b.ip_port]]}
    @a.sync_do{@a.add_member <+ [[2, @c.ip_port]]}
    @a.sync_do{@a.add_member <+ [[3, @d.ip_port]]}

    @b.sync_do{@b.add_member <+ [[0, @a.ip_port]]}
    @b.sync_do{@b.add_member <+ [[1, @b.ip_port]]}
    @b.sync_do{@b.add_member <+ [[2, @c.ip_port]]}
    @b.sync_do{@b.add_member <+ [[3, @d.ip_port]]}

    @c.sync_do{@c.add_member <+ [[0, @a.ip_port]]}
    @c.sync_do{@c.add_member <+ [[1, @b.ip_port]]}
    @c.sync_do{@c.add_member <+ [[2, @c.ip_port]]}
    @c.sync_do{@c.add_member <+ [[3, @d.ip_port]]}

    @d.sync_do{@d.add_member <+ [[0, @a.ip_port]]}
    @d.sync_do{@d.add_member <+ [[1, @b.ip_port]]}
    @d.sync_do{@d.add_member <+ [[2, @c.ip_port]]}
    @d.sync_do{@d.add_member <+ [[3, @d.ip_port]]}

    @a.sync_do{@a.quorum_config <+ [[0.5,0.5]]}
    @b.sync_do{@b.quorum_config <+ [[0.5,0.5]]}
    @c.sync_do{@c.quorum_config <+ [[0.5,0.5]]}
    @d.sync_do{@d.quorum_config <+ [[0.5,0.5]]}
    tick
    @a.sync_do{@a.kvput <+ [[@b.ip_port, "key", 1, "value"]]}
    @a.sync_do{@a.kvput <+ [[@b.ip_port, "key2", 2, "value2"]]}
    tick
    # @a.sync_do{@a.kvput <+ [[@b.ip_port, "key", 3, "MREEH"]]}
    tick

    @a.sync_do{@a.kvget <+ [[4, "key2"]]}
    @a.sync_do{@a.kvget <+ [[5, "key"]]}
    tick
    tick
    @a.sync_do{@a.kvput <+ [[@b.ip_port, "key2", 6, "COUCH"]]}
    tick
    @a.sync_do{@a.kvget <+ [[7, "key2"]]}
    tick
    tick
    @a.sync_do{@a.kvput <+ [[@b.ip_port, "key", 8, "WAVES"]]}
    tick
    tick
    @a.sync_do{@a.kvput <+ [[@b.ip_port, "key", 9, "WAVES"]]}
    # @a.sync_do{@a.kvput <+ [[@b.ip_port, "key", 11, "WAVES"]]}
    # @a.sync_do{@a.kvput <+ [[@b.ip_port, "key", 12, "asdf"]]}
    tick
    tick
    @a.sync_do{@a.kvget <+ [[13, "key"]]}
    tick
    tick
    # @a.sync_do{@a.kvget <+ [[8, "key"]]}
    # @a.sync_do{@a.kvput <+ [[@b.ip_port, "key2", 2, "value2"]]}
    # tick
    # @a.sync_do{@a.kvget <+ [[3, "yoyoyo"]]}
    # @a.sync_do{@a.kvget <+ [[4, "key2"]]}

  end
end
