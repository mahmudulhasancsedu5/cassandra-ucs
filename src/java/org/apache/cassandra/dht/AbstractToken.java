package org.apache.cassandra.dht;

abstract class AbstractToken<C> extends Token
{
    private static final long serialVersionUID = 1L;

    final C token;

    protected AbstractToken(C token)
    {
        this.token = token;
    }

    @Override
    public String toString()
    {
        return token.toString();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null || this.getClass() != obj.getClass())
            return false;

        return token.equals(((AbstractToken<?>)obj).token);
    }

    @Override
    public int hashCode()
    {
        return token.hashCode();
    }
}
